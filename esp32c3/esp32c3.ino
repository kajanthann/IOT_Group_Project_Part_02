/***************************************************************************
 *  ESP32-C3 #2  -  AUXILIARY NODE  (single-file version, protocol merged)
 *  NO BUZZER VERSION - all feedback is done with the two status LEDs.
 *  Receives events from the Station over ESP-NOW and acts on them:
 *    - LED patterns for every attendance event
 *    - supervises the Station heartbeat (fault detection, fast red blink = OFFLINE)
 *    - button -> remote command back to the Station (PAUSE / RESUME, long = WDT test)
 *  Finds the Station's Wi-Fi channel automatically (channel hunting) and learns
 *  its MAC from the first heartbeat -> nothing to configure on this board.
 *
 *  Pins (ESP32-C3): GREEN LED=5  RED LED=6  BUTTON=7 (to GND)
 *  Tasks: Rx(4) Monitor(3) Led(2) Button(2)  + watchdog on all
 ***************************************************************************/
#include <Arduino.h>
#if defined(ESP_ARDUINO_VERSION_MAJOR) && (ESP_ARDUINO_VERSION_MAJOR < 3)
  #error "Please use arduino-esp32 core 3.x"
#endif

#include <stdint.h>
#include <WiFi.h>
#include <esp_now.h>
#include <esp_wifi.h>
#include <esp_task_wdt.h>
#include <esp_idf_version.h>
#include <Preferences.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"
#include "freertos/event_groups.h"

// ======================================================================
//  ESP-NOW protocol between Station (#1) and Aux node (#2)
//  (formerly fp_protocol.h)
// ======================================================================
#define FP_MAGIC     0xA5
#define FP_NAME_LEN  20
#define FP_REG_LEN   15

enum FpMsgType : uint8_t {
  MSG_HEARTBEAT = 1,   // #1 -> #2  every 1 s : link supervision + status flags
  MSG_ATT_EVENT = 2,   // #1 -> #2  attendance / enrollment result
  MSG_CMD       = 3,   // #2 -> #1  remote command
  MSG_ACK       = 4    // both ways, seq = acknowledged sequence number
};

enum FpHbFlags : uint8_t {
  HBF_WIFI = 0x01, HBF_MQTT = 0x02, HBF_NTP = 0x04, HBF_PAUSED = 0x08, HBF_ENROLL = 0x10
};

enum FpResult : uint8_t {
  RES_MATCH = 0, RES_NOMATCH = 1, RES_OFFLINE_SAVED = 2, RES_ENROLL_OK = 3, RES_ENROLL_FAIL = 4
};

enum FpCommand : uint8_t { CMD_RESUME = 0, CMD_PAUSE = 1, CMD_WDT_TEST = 2 };

typedef struct __attribute__((packed)) {
  uint8_t  magic;
  uint8_t  type;
  uint8_t  seq;
  uint8_t  value;                 // flags / result / command
  uint32_t uptimeS;
  uint8_t  studentId;
  char     name[FP_NAME_LEN];
  char     reg[FP_REG_LEN];
} FpMsg;

static_assert(sizeof(FpMsg) == 44, "FpMsg size changed");

// ---- ESP-NOW callback signature differs between IDF versions ----
#if ESP_IDF_VERSION >= ESP_IDF_VERSION_VAL(5, 0, 0)
  #define ESPNOW_RECV_SIG  const esp_now_recv_info_t *info, const uint8_t *data, int len
  #define ESPNOW_SRC_MAC   (info->src_addr)
#else
  #define ESPNOW_RECV_SIG  const uint8_t *mac_addr, const uint8_t *data, int len
  #define ESPNOW_SRC_MAC   (mac_addr)
#endif

// ======================================================================
//  Aux node application
// ======================================================================
#define LED_GREEN   5
#define LED_RED     6
#define BTN_PIN     7

#define WDT_TIMEOUT_S    8
#define LINK_TIMEOUT_MS  5000
#define EVT_LINK         (1 << 0)

// one-shot LED patterns (replace the old buzzer patterns)
enum LedPattern : uint8_t {
  LP_OK = 1,        // match / enroll OK       : green 3 blinks
  LP_FAIL,          // no match / enroll fail  : red 2 long blinks
  LP_WARN,          // saved offline           : green+red together 3 blinks
  LP_LINK,          // station link restored   : both LEDs 2 quick blinks
  LP_CMD_OK,        // button command acked    : green 2 short blinks
  LP_CMD_FAIL       // button command failed   : red 2 long blinks
};

struct RxItem { uint8_t mac[6]; FpMsg msg; };

static QueueHandle_t      qRx, qLed;
static SemaphoreHandle_t  semAck;
static EventGroupHandle_t evGroup;
static TaskHandle_t       hButton;
static Preferences        prefs;

static uint8_t           peerMac[6];
static bool              peerKnown = false;
static volatile uint32_t lastHbMs = 0;
static volatile uint8_t  stationFlags = 0;
static volatile uint8_t  ackSeq = 0;
static uint8_t           txSeq = 0;
static uint8_t           currentChannel = 1, savedChannel = 1;
static bool              pausedState = false;

// ------------------------------------------------------------------ helpers
static void initWatchdog() {
#if ESP_IDF_VERSION >= ESP_IDF_VERSION_VAL(5, 0, 0)
  esp_task_wdt_config_t wc;
  wc.timeout_ms     = WDT_TIMEOUT_S * 1000;
  wc.idle_core_mask = 0;
  wc.trigger_panic  = true;
  if (esp_task_wdt_reconfigure(&wc) != ESP_OK) esp_task_wdt_init(&wc);
#else
  esp_task_wdt_init(WDT_TIMEOUT_S, true);
#endif
}

static void setChannel(uint8_t ch) {
  esp_wifi_set_promiscuous(true);
  esp_wifi_set_channel(ch, WIFI_SECOND_CHAN_NONE);
  esp_wifi_set_promiscuous(false);
  currentChannel = ch;
}

static void learnPeer(const uint8_t *mac) {
  if (peerKnown && memcmp(mac, peerMac, 6) == 0) return;
  if (peerKnown) esp_now_del_peer(peerMac);
  memcpy(peerMac, mac, 6);
  esp_now_peer_info_t p; memset(&p, 0, sizeof(p));
  memcpy(p.peer_addr, mac, 6);
  p.channel = 0;
  p.encrypt = false;
  p.ifidx   = WIFI_IF_STA;
  if (esp_now_add_peer(&p) == ESP_OK) {
    peerKnown = true;
    Serial.printf("[ESP-NOW] Station learned: %02X:%02X:%02X:%02X:%02X:%02X\n",
                  mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
  }
}

static void sendAck(uint8_t seq) {
  if (!peerKnown) return;
  FpMsg a; memset(&a, 0, sizeof(a));
  a.magic = FP_MAGIC; a.type = MSG_ACK; a.seq = seq; a.uptimeS = millis() / 1000;
  esp_now_send(peerMac, (uint8_t *)&a, sizeof(a));
}

static bool sendCmd(uint8_t cmd) {                      // reliable: 3 tries, ACK required
  if (!peerKnown) return false;
  FpMsg m; memset(&m, 0, sizeof(m));
  m.magic = FP_MAGIC; m.type = MSG_CMD; m.seq = ++txSeq; m.value = cmd; m.uptimeS = millis() / 1000;
  for (int t = 0; t < 3; t++) {
    xSemaphoreTake(semAck, 0);
    esp_now_send(peerMac, (uint8_t *)&m, sizeof(m));
    uint32_t t0 = millis();
    while ((millis() - t0) < 120) {
      if (xSemaphoreTake(semAck, pdMS_TO_TICKS(10)) == pdTRUE && ackSeq == m.seq) return true;
    }
  }
  return false;
}

static void led(LedPattern p) { uint8_t v = p; xQueueSend(qLed, &v, 0); }

// ------------------------------------------------------------------ ESP-NOW receive callback
static void onRecv(ESPNOW_RECV_SIG) {
  if (len != (int)sizeof(FpMsg)) return;
  const FpMsg *m = (const FpMsg *)data;
  if (m->magic != FP_MAGIC) return;
  const uint8_t *src = ESPNOW_SRC_MAC;
  if (m->type == MSG_ACK) {
    if (peerKnown && memcmp(src, peerMac, 6) == 0) { ackSeq = m->seq; xSemaphoreGive(semAck); }
    return;
  }
  RxItem it; memcpy(it.mac, src, 6); it.msg = *m;
  xQueueSend(qRx, &it, 0);
}

// ------------------------------------------------------------------ tasks
static void onAttendanceEvent(const FpMsg &m) {
  Serial.printf("[EVT] result=%u id=%u name=%s\n", m.value, m.studentId, m.name);
  switch (m.value) {
    case RES_MATCH:         led(LP_OK);   break;
    case RES_NOMATCH:       led(LP_FAIL); break;
    case RES_OFFLINE_SAVED: led(LP_WARN); break;
    case RES_ENROLL_OK:     led(LP_OK);   break;
    case RES_ENROLL_FAIL:   led(LP_FAIL); break;
  }
}

static void RxTask(void *) {
  esp_task_wdt_add(NULL);
  RxItem it;
  for (;;) {
    esp_task_wdt_reset();
    if (xQueueReceive(qRx, &it, pdMS_TO_TICKS(500)) != pdTRUE) continue;
    learnPeer(it.mac);
    if (it.msg.type == MSG_HEARTBEAT) {
      lastHbMs = millis();
      stationFlags = it.msg.value;
      xEventGroupSetBits(evGroup, EVT_LINK);
      if (currentChannel != savedChannel) {              // remember the working channel (NVS)
        savedChannel = currentChannel;
        prefs.putUChar("ch", savedChannel);
        Serial.printf("[ESP-NOW] Locked on channel %u (saved to NVS)\n", savedChannel);
      }
    } else if (it.msg.type == MSG_ATT_EVENT) {
      onAttendanceEvent(it.msg);
    }
    sendAck(it.msg.seq);
  }
}

static void MonitorTask(void *) {
  esp_task_wdt_add(NULL);
  bool wasAlive = false, everAlive = false;
  for (;;) {
    esp_task_wdt_reset();
    uint32_t now = millis();
    bool alive = lastHbMs && ((now - lastHbMs) < LINK_TIMEOUT_MS);
    if (alive) {
      xEventGroupSetBits(evGroup, EVT_LINK);
      if (!wasAlive) { Serial.println("[LINK] Station ONLINE"); if (everAlive) led(LP_LINK); everAlive = true; }
      wasAlive = true;
      vTaskDelay(pdMS_TO_TICKS(250));
      continue;
    }
    // link down: the LED task shows the alarm (fast red blink) while EVT_LINK is cleared
    xEventGroupClearBits(evGroup, EVT_LINK);
    if (wasAlive) { Serial.println("[LINK] Station OFFLINE (no heartbeat)"); wasAlive = false; }

    // channel hunting: station follows the router channel, so sweep 1..13 until a heartbeat arrives
    bool lostLong = lastHbMs ? ((now - lastHbMs) > 8000) : (now > 4000);
    if (lostLong) {
      uint8_t ch = (currentChannel % 13) + 1;
      setChannel(ch);
      Serial.printf("[LINK] searching on channel %u\n", ch);
      vTaskDelay(pdMS_TO_TICKS(1500));
    } else {
      vTaskDelay(pdMS_TO_TICKS(250));
    }
  }
}

// ---- LED output: pulse n times (LEDs are forced off between pulses) ----
static void pulse(bool g, bool r, int onMs, int offMs, int n) {
  for (int i = 0; i < n; i++) {
    digitalWrite(LED_GREEN, g ? HIGH : LOW);
    digitalWrite(LED_RED,   r ? HIGH : LOW);
    vTaskDelay(pdMS_TO_TICKS(onMs));
    digitalWrite(LED_GREEN, LOW);
    digitalWrite(LED_RED,   LOW);
    vTaskDelay(pdMS_TO_TICKS(offMs));
    esp_task_wdt_reset();
  }
}

static void playPattern(uint8_t p) {
  digitalWrite(LED_GREEN, LOW);
  digitalWrite(LED_RED, LOW);
  switch (p) {
    case LP_OK:       pulse(true,  false, 120, 120, 3); break;
    case LP_FAIL:     pulse(false, true,  350, 150, 2); break;
    case LP_WARN:     pulse(true,  true,  100, 100, 3); break;
    case LP_LINK:     pulse(true,  true,   60,  60, 2); break;
    case LP_CMD_OK:   pulse(true,  false,  80,  80, 2); break;
    case LP_CMD_FAIL: pulse(false, true,  350, 150, 2); break;
  }
}

// Status display (runs between patterns, 125 ms tick):
//   link down  -> red FAST blink, green off            (STATION OFFLINE alarm)
//   link up    -> green solid (cloud OK) / slow blink (cloud down)
//                 red solid = station paused
static void LedTask(void *) {
  esp_task_wdt_add(NULL);
  uint32_t n = 0;
  for (;;) {
    esp_task_wdt_reset();
    uint8_t p;
    if (xQueueReceive(qLed, &p, pdMS_TO_TICKS(125)) == pdTRUE) playPattern(p);
    n++;
    bool fast = (n & 1);               // toggles every tick  (~4 Hz)
    bool slow = ((n >> 2) & 1);        // toggles every 4 ticks (~1 Hz)
    bool link = (xEventGroupGetBits(evGroup) & EVT_LINK) != 0;
    if (!link) {
      digitalWrite(LED_GREEN, LOW);
      digitalWrite(LED_RED, fast);
    } else {
      bool cloud  = (stationFlags & HBF_MQTT) != 0;
      bool paused = (stationFlags & HBF_PAUSED) != 0;
      digitalWrite(LED_GREEN, cloud ? HIGH : slow);
      digitalWrite(LED_RED, paused ? HIGH : LOW);
    }
  }
}

static void IRAM_ATTR isrButton() {
  static uint32_t last = 0;
  uint32_t now = millis();
  if (now - last < 200) return;
  last = now;
  BaseType_t hpw = pdFALSE;
  vTaskNotifyGiveFromISR(hButton, &hpw);
  portYIELD_FROM_ISR(hpw);
}

static void ButtonTask(void *) {
  esp_task_wdt_add(NULL);
  for (;;) {
    esp_task_wdt_reset();
    if (ulTaskNotifyTake(pdTRUE, pdMS_TO_TICKS(1000)) == 0) continue;
    vTaskDelay(pdMS_TO_TICKS(30));
    if (digitalRead(BTN_PIN) != LOW) continue;
    uint32_t t0 = millis();
    while (digitalRead(BTN_PIN) == LOW && (millis() - t0) < 2500) { esp_task_wdt_reset(); vTaskDelay(pdMS_TO_TICKS(20)); }
    bool ok;
    if ((millis() - t0) >= 2500) {
      Serial.println("[BTN] long press -> CMD_WDT_TEST");
      ok = sendCmd(CMD_WDT_TEST);
      while (digitalRead(BTN_PIN) == LOW) { esp_task_wdt_reset(); vTaskDelay(pdMS_TO_TICKS(50)); }
    } else {
      pausedState = !pausedState;
      Serial.printf("[BTN] short press -> %s\n", pausedState ? "CMD_PAUSE" : "CMD_RESUME");
      ok = sendCmd(pausedState ? CMD_PAUSE : CMD_RESUME);
      if (!ok) pausedState = !pausedState;               // command failed, keep old state
    }
    led(ok ? LP_CMD_OK : LP_CMD_FAIL);
  }
}

// ------------------------------------------------------------------ setup
void setup() {
  Serial.begin(115200);
  delay(600);
  pinMode(LED_GREEN, OUTPUT);  digitalWrite(LED_GREEN, LOW);
  pinMode(LED_RED, OUTPUT);    digitalWrite(LED_RED, LOW);
  pinMode(BTN_PIN, INPUT_PULLUP);

  prefs.begin("fpnode", false);
  savedChannel = prefs.getUChar("ch", 1);
  if (savedChannel < 1 || savedChannel > 13) savedChannel = 1;

  WiFi.mode(WIFI_STA);
  WiFi.disconnect();
  Serial.println("\n--- ESP32-C3 Aux Node ---");
  Serial.print("MAC (put this in the Station config portal): ");
  Serial.println(WiFi.macAddress());

  qRx     = xQueueCreate(10, sizeof(RxItem));
  qLed    = xQueueCreate(8, sizeof(uint8_t));
  semAck  = xSemaphoreCreateBinary();
  evGroup = xEventGroupCreate();

  initWatchdog();
  if (esp_now_init() != ESP_OK) { Serial.println("[ESP-NOW] init failed"); delay(2000); ESP.restart(); }
  esp_now_register_recv_cb(onRecv);
  setChannel(savedChannel);

  xTaskCreate(RxTask,      "Rx",      4096, NULL, 4, NULL);
  xTaskCreate(MonitorTask, "Monitor", 4096, NULL, 3, NULL);
  xTaskCreate(LedTask,     "Led",     2048, NULL, 2, NULL);
  xTaskCreate(ButtonTask,  "Button",  3072, NULL, 2, &hButton);
  attachInterrupt(digitalPinToInterrupt(BTN_PIN), isrButton, FALLING);   // after hButton exists

  led(LP_LINK);   // boot indication
}

void loop() {
  vTaskDelete(NULL);
}