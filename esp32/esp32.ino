/***************************************************************************
 *  ESP32 #1  -  MAIN STATION   (classic ESP32 DevKit, arduino-esp32 core 3.x)
 *  CSE4052 Part 2 - Fingerprint Attendance System
 *  (single-file version, protocol merged)
 *
 *  A1  FreeRTOS tasks      : Fingerprint, Display, Led, Net, Storage, Comm,
 *                            Heartbeat, Button (+ Config in config mode)
 *  A2  Config portal       : hold button on reset -> SoftAP + HTML + WebSocket
 *  A3  Interrupts          : button GPIO ISR, 1 Hz hardware-timer ISR,
 *                            optional AS608 WAKEUP pin ISR
 *  A4  Distributed         : ESP-NOW link to ESP32-C3 aux node (ACK + retry)
 *  B1  NVS (Preferences)   : all settings
 *  B2  Task watchdog       : every task subscribed, hang demo
 *  B4  Inter-task comms    : queues, mutexes, event group, task notifications,
 *                            binary semaphore
 *
 *  Pins : AS608 RX=18 TX=19 | OLED SDA=21 SCL=22 | GREEN=2 RED=4
 *         Button=13 (to GND, INPUT_PULLUP)  | AS608 WAKEUP=27 (optional)
 ***************************************************************************/
#include <Arduino.h>
#if defined(ESP_ARDUINO_VERSION_MAJOR) && (ESP_ARDUINO_VERSION_MAJOR < 3)
  #error "Please use arduino-esp32 core 3.x"
#endif

#include <stdint.h>
#include <Wire.h>
#include <Adafruit_SH110X.h>
#include <Adafruit_Fingerprint.h>
#include <HardwareSerial.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <Preferences.h>
#include <EEPROM.h>
#include <WebServer.h>
#include <WebSocketsServer.h>
#include <esp_now.h>
#include <esp_wifi.h>
#include <esp_task_wdt.h>
#include <esp_system.h>
#include <esp_idf_version.h>
#include "esp_sntp.h"
#include "time.h"
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
//  Station application
// ======================================================================

// ============================== PINS / CONSTANTS ==============================
#define OLED_SDA     21
#define OLED_SCL     22
#define RX_PIN       18
#define TX_PIN       19
#define GREEN_LED    2
#define RED_LED      4
#define BTN_PIN      13        // do NOT use GPIO0 (download mode)
#define WAKE_PIN     27        // AS608 WAKEUP/touch pin (6-wire modules only)

#define SCREEN_WIDTH  128
#define SCREEN_HEIGHT 128

#define WDT_TIMEOUT_S 10

// NTP
static const char *ntpServer  = "pool.ntp.org";
static const char *ntpServer2 = "time.cloudflare.com";
static const long  gmtOffset_sec = 19800;
static const int   daylightOffset_sec = 0;
#define TZ_SUFFIX "+05:30"
#define NTP_STALE_MS 7200000UL

// EEPROM layout (same as Part 1 -> existing students are kept)
#define EEPROM_SIZE             4096
#define MAX_STUDENTS            50
#define MAX_OFFLINE_ATTENDANCE  30
#define STUDENT_NAME_LEN        20
#define STUDENT_REG_LEN         15
#define TS_LEN                  26
#define STUDENT_RECORD_SIZE     (1 + STUDENT_NAME_LEN + STUDENT_REG_LEN)
#define STUDENTS_EEPROM_SIZE    (1 + (MAX_STUDENTS * STUDENT_RECORD_SIZE))
#define OFFLINE_RECORD_SIZE     (1 + STUDENT_NAME_LEN + STUDENT_REG_LEN + TS_LEN)
#define OFFLINE_EEPROM_SIZE     (1 + (MAX_OFFLINE_ATTENDANCE * OFFLINE_RECORD_SIZE))
#define OFFLINE_START_ADDR      (STUDENTS_EEPROM_SIZE)
#if (STUDENTS_EEPROM_SIZE + OFFLINE_EEPROM_SIZE) > EEPROM_SIZE
  #error "EEPROM layout exceeds EEPROM_SIZE"
#endif

// MQTT topics (unchanged -> Node.js bridge + React dashboard need no change)
enum MqttTopicId : uint8_t { T_ATTENDANCE = 0, T_ENROLLED, T_HEARTBEAT, T_MESSAGE, T_STATE_PUB };
static const char *TOPIC_STR[] = { "fp/attendance", "fp/enrolled", "fp/heartbeat", "fp/message", "fp/stateAck" };
#define TOPIC_SYS_STATE   "fp/systemState"
#define TOPIC_ENROLL_DATA "fp/enrollData"
#define MQTT_BUF_SIZE 512

// Event group bits
#define EVT_WIFI_UP    (1 << 0)
#define EVT_MQTT_UP    (1 << 1)
#define EVT_NTP_OK     (1 << 2)
#define EVT_PEER_LINK  (1 << 3)
#define EVT_NTP_REQ    (1 << 4)

typedef StaticJsonDocument<1536> CfgDoc;

// ============================== TYPES ==============================
struct Student    { uint8_t id; char name[STUDENT_NAME_LEN]; char regNum[STUDENT_REG_LEN]; };
struct Attendance { uint8_t id; char name[STUDENT_NAME_LEN]; char regNum[STUDENT_REG_LEN]; char timestamp[TS_LEN]; };

enum SystemState : uint8_t { VERIFY, ENROLL };

enum DisplayKind : uint8_t { DM_BOTTOM = 1, DM_IDLE, DM_STATE, DM_PROGRESS, DM_TICK, DM_DIAG };
struct DisplayMsg { uint8_t kind; uint8_t value; char text[48]; };   // value: ttl in 100 ms units (0 = sticky)
struct LedMsg     { uint8_t pin; uint16_t ms; };
struct CommItem   { uint8_t type; uint8_t value; uint8_t id; char name[STUDENT_NAME_LEN]; char reg[STUDENT_REG_LEN]; };
struct MqttOut    { uint8_t topic; bool retained; char payload[320]; };

enum StoreOpKind : uint8_t { OP_ADD_OFFLINE = 1, OP_SAVE_STUDENTS, OP_REMOVE_AT };
struct StoreOp    { uint8_t op; uint8_t idx; Attendance rec; };

enum FpCmdKind : uint8_t { FC_NONE = 0, FC_STATE, FC_ENROLL_DATA, FC_PAUSE, FC_RESUME };
struct FpCmd      { uint8_t kind; char text[16]; char name[STUDENT_NAME_LEN]; char reg[STUDENT_REG_LEN]; };

struct Config {
  char     deviceName[24];
  char     wifiSsid[33];
  char     wifiPass[65];
  char     mqttBroker[96];
  uint16_t mqttPort;
  char     mqttUser[48];
  char     mqttPass[64];
  uint16_t hbIntervalSec;
  uint16_t welcomeHoldMs;
  uint16_t scanPollMs;
  uint8_t  useWakePin;
  char     peerMac[18];
};

// ============================== GLOBALS ==============================
static Config      cfg;
static Preferences prefs;

Adafruit_SH1107 display(SCREEN_WIDTH, SCREEN_HEIGHT, &Wire);
HardwareSerial  mySerial(1);
Adafruit_Fingerprint finger(&mySerial);

WiFiClientSecure wifiSecure;
PubSubClient     mqttClient(wifiSecure);

static Student    students[MAX_STUDENTS];
static uint8_t    studentCount = 0;
static Attendance offlineAttendance[MAX_OFFLINE_ATTENDANCE];
static volatile uint8_t offlineCount = 0;

static QueueHandle_t      qDisplay, qLed, qComm, qEspRx, qMqttOut, qAtt, qStore, qFpCmd;
static SemaphoreHandle_t  mtxI2C, mtxData, semAck;
static EventGroupHandle_t evGroup;
static TaskHandle_t hFinger, hDisplay, hLed, hNet, hStorage, hComm, hHeartbeat, hButton, hConfig;
static hw_timer_t *hbTimer = nullptr;

static volatile SystemState currentState = VERIFY;
static volatile bool     scanPaused = false;
static volatile bool     wdtHangRequested = false;
static volatile uint32_t ntpSyncedAtMs = 0;

// enrollment data that arrived together with the ENROLL state (must not be dropped)
static FpCmd pendingEnroll;
static bool  havePending = false;

// statistics for the report (A3 evidence): shown on the diagnostics screen
static volatile uint32_t statScans = 0, statIsrTimer = 0, statIsrBtn = 0, statIsrWake = 0;

// ESP-NOW
static uint8_t           peerMac[6];
static bool              peerConfigured = false;
static volatile uint32_t lastPeerRxMs = 0;
static volatile uint8_t  ackSeq = 0;
static uint8_t           txSeq = 0;

// display state (touched only by DisplayTask, and by setup() before it starts)
static char     bottomMsg[48] = "";
static uint32_t bottomExpireMs = 0;
static bool     idleMode = true;
static char     stateLabel[8] = "VERIFY";
static uint8_t  progressPct = 0;
static uint32_t diagUntilMs = 0;

// ============================== SMALL HELPERS ==============================
static const char *resetReasonStr() {
  switch (esp_reset_reason()) {
    case ESP_RST_POWERON:   return "PowerOn";
    case ESP_RST_SW:        return "Software";
    case ESP_RST_PANIC:     return "Panic";
    case ESP_RST_INT_WDT:   return "IntWDT";
    case ESP_RST_TASK_WDT:  return "TaskWDT";
    case ESP_RST_WDT:       return "WDT";
    case ESP_RST_BROWNOUT:  return "Brownout";
    case ESP_RST_DEEPSLEEP: return "DeepSleep";
    default:                return "Other";
  }
}

// sleep in 200 ms slices and keep feeding the watchdog (call only from subscribed tasks)
static void delayFeed(uint32_t ms) {
  while (ms) {
    uint32_t s = (ms > 200) ? 200 : ms;
    vTaskDelay(pdMS_TO_TICKS(s));
    esp_task_wdt_reset();
    ms -= s;
  }
}

static void mqttEnqueue(uint8_t topic, const char *payload, bool retained = false) {
  MqttOut o; memset(&o, 0, sizeof(o));
  o.topic = topic; o.retained = retained;
  strlcpy(o.payload, payload, sizeof(o.payload));
  xQueueSend(qMqttOut, &o, 0);
}

static void mqttSendMsg(const char *txt) {
  StaticJsonDocument<160> d;
  d["msg"] = txt;
  char b[200];
  serializeJson(d, b, sizeof(b));
  mqttEnqueue(T_MESSAGE, b);
}

static void uiBottom(const char *txt, uint8_t ttl100ms = 0, bool toMqtt = false) {
  DisplayMsg m; memset(&m, 0, sizeof(m));
  m.kind = DM_BOTTOM; m.value = ttl100ms;
  strlcpy(m.text, txt, sizeof(m.text));
  xQueueSend(qDisplay, &m, 0);
  if (toMqtt) mqttSendMsg(txt);
}
static void uiIdle() { DisplayMsg m; memset(&m, 0, sizeof(m)); m.kind = DM_IDLE; xQueueSend(qDisplay, &m, 0); }
static void uiState(const char *s) {
  DisplayMsg m; memset(&m, 0, sizeof(m)); m.kind = DM_STATE; strlcpy(m.text, s, sizeof(m.text));
  xQueueSend(qDisplay, &m, 0);
}
static void uiProgress(uint8_t pct) {
  DisplayMsg m; memset(&m, 0, sizeof(m)); m.kind = DM_PROGRESS; m.value = pct;
  xQueueSend(qDisplay, &m, 0);
}
static void ledPulse(uint8_t pin, uint16_t ms) { LedMsg m = { pin, ms }; xQueueSend(qLed, &m, 0); }

static void commSend(uint8_t type, uint8_t value, uint8_t id = 0, const char *name = "", const char *reg = "") {
  CommItem it; memset(&it, 0, sizeof(it));
  it.type = type; it.value = value; it.id = id;
  strlcpy(it.name, name, sizeof(it.name));
  strlcpy(it.reg, reg, sizeof(it.reg));
  xQueueSend(qComm, &it, 0);
}

// ============================== TIME ==============================
static void ntpSyncCallback(struct timeval *tv) {
  ntpSyncedAtMs = millis();
  xEventGroupSetBits(evGroup, EVT_NTP_OK);
  Serial.printf("[NTP] Synced, epoch=%llu\n", (unsigned long long)tv->tv_sec);
}
static bool isTimeSynced() { return (xEventGroupGetBits(evGroup) & EVT_NTP_OK) != 0; }

static bool getTimestampStr(char *out, size_t n) {
  if (!isTimeSynced()) return false;
  struct tm ti;
  if (!getLocalTime(&ti, 20)) return false;
  if (ti.tm_year < 120) return false;
  strftime(out, n, "%Y-%m-%dT%H:%M:%S" TZ_SUFFIX, &ti);
  return true;
}

// ============================== EEPROM (records) ==============================
static bool safeEEPROMWrite(int addr, const uint8_t *buf, size_t len) {
  if (addr < 0 || (addr + (int)len) > EEPROM_SIZE) return false;
  for (size_t i = 0; i < len; i++) EEPROM.write(addr + i, buf[i]);
  return true;
}
static void saveStudentsToEEPROM() {
  int addr = 0;
  EEPROM.write(addr++, studentCount);
  for (int i = 0; i < studentCount; i++) {
    safeEEPROMWrite(addr, (uint8_t *)&students[i].id, 1);                  addr += 1;
    safeEEPROMWrite(addr, (uint8_t *)students[i].name, STUDENT_NAME_LEN);  addr += STUDENT_NAME_LEN;
    safeEEPROMWrite(addr, (uint8_t *)students[i].regNum, STUDENT_REG_LEN); addr += STUDENT_REG_LEN;
  }
  EEPROM.commit();
  Serial.printf("[EEPROM] Students saved: %d\n", studentCount);
}
static void loadStudentsFromEEPROM() {
  int addr = 0;
  uint8_t cnt = EEPROM.read(addr++);
  studentCount = (cnt > MAX_STUDENTS) ? 0 : cnt;
  for (int i = 0; i < studentCount; i++) {
    students[i].id = EEPROM.read(addr++);
    for (int j = 0; j < STUDENT_NAME_LEN; j++) students[i].name[j]   = EEPROM.read(addr++);
    for (int j = 0; j < STUDENT_REG_LEN;  j++) students[i].regNum[j] = EEPROM.read(addr++);
    students[i].name[STUDENT_NAME_LEN - 1]  = '\0';
    students[i].regNum[STUDENT_REG_LEN - 1] = '\0';
  }
  Serial.printf("[EEPROM] Loaded %d students\n", studentCount);
}
static void saveOfflineAttendanceToEEPROM() {
  int addr = OFFLINE_START_ADDR;
  EEPROM.write(addr++, offlineCount);
  for (int i = 0; i < offlineCount; i++) {
    EEPROM.write(addr++, offlineAttendance[i].id);
    safeEEPROMWrite(addr, (uint8_t *)offlineAttendance[i].name,      STUDENT_NAME_LEN); addr += STUDENT_NAME_LEN;
    safeEEPROMWrite(addr, (uint8_t *)offlineAttendance[i].regNum,    STUDENT_REG_LEN);  addr += STUDENT_REG_LEN;
    safeEEPROMWrite(addr, (uint8_t *)offlineAttendance[i].timestamp, TS_LEN);           addr += TS_LEN;
  }
  EEPROM.commit();
  Serial.printf("[EEPROM] Offline saved: %d\n", offlineCount);
}
static void loadOfflineAttendanceFromEEPROM() {
  int addr = OFFLINE_START_ADDR;
  uint8_t cnt = EEPROM.read(addr++);
  if (cnt > MAX_OFFLINE_ATTENDANCE) cnt = 0;
  offlineCount = 0;
  for (int i = 0; i < cnt; i++) {
    offlineAttendance[i].id = EEPROM.read(addr++);
    for (int j = 0; j < STUDENT_NAME_LEN; j++) offlineAttendance[i].name[j]      = EEPROM.read(addr++);
    for (int j = 0; j < STUDENT_REG_LEN;  j++) offlineAttendance[i].regNum[j]    = EEPROM.read(addr++);
    for (int j = 0; j < TS_LEN;           j++) offlineAttendance[i].timestamp[j] = EEPROM.read(addr++);
    offlineAttendance[i].name[STUDENT_NAME_LEN - 1]  = '\0';
    offlineAttendance[i].regNum[STUDENT_REG_LEN - 1] = '\0';
    offlineAttendance[i].timestamp[TS_LEN - 1]       = '\0';
    offlineCount = offlineCount + 1;
  }
  Serial.printf("[EEPROM] Loaded %d offline records\n", offlineCount);
}
static void removeOfflineRecordAt(uint8_t idx) {          // caller holds mtxData
  if (idx >= offlineCount) return;
  for (uint8_t i = idx; i < offlineCount - 1; i++) offlineAttendance[i] = offlineAttendance[i + 1];
  offlineCount = offlineCount - 1;
  saveOfflineAttendanceToEEPROM();
}
static void lookupStudent(uint8_t id, char *name, char *reg) {
  strlcpy(name, "Unknown", STUDENT_NAME_LEN);
  reg[0] = '\0';
  xSemaphoreTake(mtxData, portMAX_DELAY);
  for (uint8_t i = 0; i < studentCount; i++) {
    if (students[i].id == id) {
      strlcpy(name, students[i].name, STUDENT_NAME_LEN);
      strlcpy(reg, students[i].regNum, STUDENT_REG_LEN);
      break;
    }
  }
  xSemaphoreGive(mtxData);
}

// ============================== CONFIG (B1: NVS) ==============================
static void loadConfig() {
  prefs.begin("fpcfg", false);
  prefs.getString("devName", "ESP32_Attendance").toCharArray(cfg.deviceName, sizeof(cfg.deviceName));
  prefs.getString("ssid", "").toCharArray(cfg.wifiSsid, sizeof(cfg.wifiSsid));
  prefs.getString("wpass", "").toCharArray(cfg.wifiPass, sizeof(cfg.wifiPass));
  prefs.getString("mBroker", "").toCharArray(cfg.mqttBroker, sizeof(cfg.mqttBroker));
  cfg.mqttPort = prefs.getUShort("mPort", 8883);
  prefs.getString("mUser", "").toCharArray(cfg.mqttUser, sizeof(cfg.mqttUser));
  prefs.getString("mPass", "").toCharArray(cfg.mqttPass, sizeof(cfg.mqttPass));
  cfg.hbIntervalSec = prefs.getUShort("hbSec", 2);
  cfg.welcomeHoldMs = prefs.getUShort("holdMs", 2000);
  cfg.scanPollMs    = prefs.getUShort("pollMs", 300);
  cfg.useWakePin    = prefs.getUChar("wakePin", 0);
  prefs.getString("peerMac", "").toCharArray(cfg.peerMac, sizeof(cfg.peerMac));
  prefs.end();
}
static void saveConfig(const Config &c) {
  prefs.begin("fpcfg", false);
  prefs.putString("devName", c.deviceName);
  prefs.putString("ssid", c.wifiSsid);
  prefs.putString("wpass", c.wifiPass);
  prefs.putString("mBroker", c.mqttBroker);
  prefs.putUShort("mPort", c.mqttPort);
  prefs.putString("mUser", c.mqttUser);
  prefs.putString("mPass", c.mqttPass);
  prefs.putUShort("hbSec", c.hbIntervalSec);
  prefs.putUShort("holdMs", c.welcomeHoldMs);
  prefs.putUShort("pollMs", c.scanPollMs);
  prefs.putUChar("wakePin", c.useWakePin);
  prefs.putString("peerMac", c.peerMac);
  prefs.end();
  Serial.println("[NVS] Config saved");
}
static bool cfgValid() { return cfg.wifiSsid[0] && cfg.mqttBroker[0]; }

static bool parseMac(const char *s, uint8_t *out) {
  unsigned int m[6];
  if (sscanf(s, "%x:%x:%x:%x:%x:%x", &m[0], &m[1], &m[2], &m[3], &m[4], &m[5]) != 6) return false;
  for (int i = 0; i < 6; i++) { if (m[i] > 255) return false; out[i] = (uint8_t)m[i]; }
  return true;
}

// ============================== WATCHDOG (B2) ==============================
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
  Serial.printf("[WDT] Task watchdog %d s (panic + reset)\n", WDT_TIMEOUT_S);
}

// ============================== DISPLAY ==============================
static void bootMsg(const char *line, uint32_t holdMs = 0) {
  Serial.printf("[BOOT] %s\n", line);
  xSemaphoreTake(mtxI2C, portMAX_DELAY);
  display.clearDisplay();
  display.setTextSize(1);
  display.setTextColor(SH110X_WHITE);
  display.setCursor(0, 0);  display.println("FOC-SE Attendance");
  display.setCursor(0, 20); display.println(line);
  display.display();
  xSemaphoreGive(mtxI2C);
  if (holdMs) delay(holdMs);
}

static UBaseType_t hw(TaskHandle_t h) { return h ? uxTaskGetStackHighWaterMark(h) : 0; }

static void drawDiag() {
  display.setTextSize(1);
  display.setCursor(0, 0);
  display.println("== DIAGNOSTICS ==");
  display.printf("Up:%lus Rst:%s\n", (unsigned long)(millis() / 1000), resetReasonStr());
  display.printf("Heap:%luk Min:%luk\n", (unsigned long)(ESP.getFreeHeap() / 1024), (unsigned long)(ESP.getMinFreeHeap() / 1024));
  display.printf("RSSI:%d Ch:%d\n", (int)WiFi.RSSI(), (int)WiFi.channel());
  display.printf("Offline rec:%u\n", (unsigned)offlineCount);
  display.println("Stack free (bytes):");
  display.printf("FP  %u DSP %u\n", (unsigned)hw(hFinger), (unsigned)hw(hDisplay));
  display.printf("NET %u STO %u\n", (unsigned)hw(hNet), (unsigned)hw(hStorage));
  display.printf("COM %u HB  %u\n", (unsigned)hw(hComm), (unsigned)hw(hHeartbeat));
  display.printf("BTN %u LED %u\n", (unsigned)hw(hButton), (unsigned)hw(hLed));
  display.printf("Polls:%lu\n", (unsigned long)statScans);
  display.printf("ISR T%lu B%lu W%lu\n", (unsigned long)statIsrTimer, (unsigned long)statIsrBtn, (unsigned long)statIsrWake);
}

static void drawTop(EventBits_t b) {
  display.setTextSize(1);
  display.setCursor(0, 0);   display.print((b & EVT_WIFI_UP) ? "WiFi:Ok" : "WiFi:X");
  display.setCursor(85, 0);  display.print((b & EVT_MQTT_UP) ? "MQTT:Ok" : "MQTT:X");
  display.setCursor(0, 10);
  if (!peerConfigured) display.print("Node2:--");
  else                 display.print((b & EVT_PEER_LINK) ? "Node2:Ok" : "Node2:X");

  char buf[22]; struct tm t;
  if ((b & EVT_NTP_OK) && getLocalTime(&t, 10)) strftime(buf, sizeof(buf), "%d-%m | %H:%M:%S", &t);
  else strlcpy(buf, "No time sync", sizeof(buf));
  int16_t x1, y1; uint16_t w, h;
  display.getTextBounds(buf, 0, 22, &x1, &y1, &w, &h);
  display.setCursor((SCREEN_WIDTH - w) / 2, 22);
  display.print(buf);

  display.setTextSize(2);
  display.getTextBounds("FOC - SE", 0, 40, &x1, &y1, &w, &h);
  display.setCursor((SCREEN_WIDTH - w) / 2, 40);
  display.print("FOC - SE");
  display.setTextSize(1);
  display.drawLine(0, SCREEN_HEIGHT / 2 - 1, SCREEN_WIDTH, SCREEN_HEIGHT / 2 - 1, SH110X_WHITE);
}

static void drawBottom(EventBits_t b) {
  const int top = SCREEN_HEIGHT / 2;
  int16_t x1, y1; uint16_t w, h;
  display.setTextSize(2);
  display.getTextBounds(stateLabel, 0, top + 2, &x1, &y1, &w, &h);
  display.setCursor((SCREEN_WIDTH - w) / 2, top + 2);
  display.print(stateLabel);
  display.setTextSize(1);
  display.setCursor(0, top + 22);
  if (idleMode) display.print((b & EVT_NTP_OK) ? "Place finger..." : "No time sync!");
  else          display.print(bottomMsg);
  if (!(b & EVT_WIFI_UP) || !(b & EVT_MQTT_UP)) {
    display.setCursor(0, SCREEN_HEIGHT - 22);
    display.print("Offline: ");
    display.print((unsigned)offlineCount);
  }
  if (progressPct > 0) {
    const int bw = SCREEN_WIDTH - 4;
    int fw = (bw * progressPct) / 100;
    display.drawRect(2, SCREEN_HEIGHT - 12, bw, 10, SH110X_WHITE);
    if (fw > 0) display.fillRect(2, SCREEN_HEIGHT - 12, fw, 10, SH110X_WHITE);
  }
}

static void renderDisplay() {
  if (xSemaphoreTake(mtxI2C, pdMS_TO_TICKS(500)) != pdTRUE) return;
  display.clearDisplay();
  display.setTextColor(SH110X_WHITE);
  EventBits_t b = xEventGroupGetBits(evGroup);
  if (diagUntilMs && (int32_t)(millis() - diagUntilMs) < 0) drawDiag();
  else { diagUntilMs = 0; drawTop(b); drawBottom(b); }
  display.display();
  xSemaphoreGive(mtxI2C);
}

static void DisplayTask(void *) {
  esp_task_wdt_add(NULL);
  renderDisplay();
  for (;;) {
    esp_task_wdt_reset();
    TickType_t waitTicks = pdMS_TO_TICKS(1000);
    if (bottomExpireMs) {
      int32_t left = (int32_t)(bottomExpireMs - millis());
      if (left <= 0) waitTicks = 0;
      else { if (left > 1000) left = 1000; waitTicks = pdMS_TO_TICKS(left); }
    }
    DisplayMsg m;
    if (xQueueReceive(qDisplay, &m, waitTicks) == pdTRUE) {
      switch (m.kind) {
        case DM_BOTTOM:
          strlcpy(bottomMsg, m.text, sizeof(bottomMsg));
          idleMode = false;
          bottomExpireMs = m.value ? (millis() + (uint32_t)m.value * 100UL) : 0;
          break;
        case DM_IDLE:     idleMode = true; bottomExpireMs = 0; break;
        case DM_STATE:    strlcpy(stateLabel, m.text, sizeof(stateLabel)); break;
        case DM_PROGRESS: progressPct = m.value; break;
        case DM_DIAG:     diagUntilMs = millis() + 8000; break;
        default: break;                                        // DM_TICK -> just redraw
      }
    }
    if (bottomExpireMs && (int32_t)(millis() - bottomExpireMs) >= 0) { idleMode = true; bottomExpireMs = 0; }
    renderDisplay();
  }
}

// ============================== LED / STORAGE / HEARTBEAT / BUTTON TASKS ==============================
static void LedTask(void *) {
  esp_task_wdt_add(NULL);
  LedMsg m;
  for (;;) {
    esp_task_wdt_reset();
    if (xQueueReceive(qLed, &m, pdMS_TO_TICKS(1000)) == pdTRUE) {
      digitalWrite(m.pin, HIGH);
      vTaskDelay(pdMS_TO_TICKS(m.ms));
      digitalWrite(m.pin, LOW);
    }
  }
}

static void StorageTask(void *) {
  esp_task_wdt_add(NULL);
  StoreOp op;
  for (;;) {
    esp_task_wdt_reset();
    if (xQueueReceive(qStore, &op, pdMS_TO_TICKS(1000)) != pdTRUE) continue;
    xSemaphoreTake(mtxData, portMAX_DELAY);
    switch (op.op) {
      case OP_ADD_OFFLINE:
        if (offlineCount < MAX_OFFLINE_ATTENDANCE) {
          offlineAttendance[offlineCount] = op.rec;
          offlineCount = offlineCount + 1;
          saveOfflineAttendanceToEEPROM();
        } else {
          uiBottom("Offline full!", 20);
          Serial.println("[Store] Offline buffer full");
        }
        break;
      case OP_SAVE_STUDENTS: saveStudentsToEEPROM(); break;
      case OP_REMOVE_AT:     removeOfflineRecordAt(op.idx); break;
    }
    xSemaphoreGive(mtxData);
  }
}

// ---- Interrupts (A3): ISRs only notify a task, all work happens in the task ----
static void IRAM_ATTR isrTimer1Hz() {
  statIsrTimer = statIsrTimer + 1;
  BaseType_t hpw = pdFALSE;
  vTaskNotifyGiveFromISR(hHeartbeat, &hpw);
  portYIELD_FROM_ISR(hpw);
}
static void IRAM_ATTR isrButton() {
  static uint32_t last = 0;
  uint32_t now = millis();
  if (now - last < 200) return;                      // ISR-level debounce
  last = now;
  statIsrBtn = statIsrBtn + 1;
  BaseType_t hpw = pdFALSE;
  vTaskNotifyGiveFromISR(hButton, &hpw);
  portYIELD_FROM_ISR(hpw);
}
static void IRAM_ATTR isrWake() {
  static uint32_t last = 0;
  uint32_t now = millis();
  if (now - last < 20) return;
  last = now;
  statIsrWake = statIsrWake + 1;
  BaseType_t hpw = pdFALSE;
  vTaskNotifyGiveFromISR(hFinger, &hpw);
  portYIELD_FROM_ISR(hpw);
}

static void HeartbeatTask(void *) {
  esp_task_wdt_add(NULL);
  uint32_t sec = 0;
  for (;;) {
    // blocks (0 % CPU) until the 1 Hz hardware-timer ISR notifies; timeout only to feed the WDT
    if (ulTaskNotifyTake(pdTRUE, pdMS_TO_TICKS(2000)) == 0) { esp_task_wdt_reset(); continue; }
    esp_task_wdt_reset();

    if (wdtHangRequested) {                          // watchdog demo: stop feeding the WDT
      Serial.println("[WDT-DEMO] HeartbeatTask blocked on purpose -> reset in ~10 s");
      uiBottom("WDT demo: task hung", 0);
      for (;;) vTaskDelay(pdMS_TO_TICKS(1000));
    }
    sec++;

    DisplayMsg dm; memset(&dm, 0, sizeof(dm)); dm.kind = DM_TICK;
    xQueueSend(qDisplay, &dm, 0);

    EventBits_t b = xEventGroupGetBits(evGroup);
    uint8_t f = 0;
    if (b & EVT_WIFI_UP) f |= HBF_WIFI;
    if (b & EVT_MQTT_UP) f |= HBF_MQTT;
    if (b & EVT_NTP_OK)  f |= HBF_NTP;
    if (scanPaused)      f |= HBF_PAUSED;
    if (currentState == ENROLL) f |= HBF_ENROLL;
    commSend(MSG_HEARTBEAT, f);

    if (cfg.hbIntervalSec && (sec % cfg.hbIntervalSec) == 0) {
      char ts[TS_LEN];
      if (getTimestampStr(ts, sizeof(ts))) {
        StaticJsonDocument<128> hb;
        hb["ts"] = ts; hb["synced"] = true;
        char out[160];
        serializeJson(hb, out, sizeof(out));
        mqttEnqueue(T_HEARTBEAT, out);
      }
    }
    if ((sec % 3600) == 0) xEventGroupSetBits(evGroup, EVT_NTP_REQ);
    if ((b & EVT_NTP_OK) && (millis() - ntpSyncedAtMs) > NTP_STALE_MS) {
      xEventGroupClearBits(evGroup, EVT_NTP_OK);
      Serial.println("[NTP] Sync stale");
    }
  }
}

static void ButtonTask(void *) {
  esp_task_wdt_add(NULL);
  for (;;) {
    esp_task_wdt_reset();
    if (ulTaskNotifyTake(pdTRUE, pdMS_TO_TICKS(1000)) == 0) continue;
    vTaskDelay(pdMS_TO_TICKS(30));                               // software debounce
    if (digitalRead(BTN_PIN) != LOW) continue;
    uint32_t t0 = millis();
    while (digitalRead(BTN_PIN) == LOW && (millis() - t0) < 3000) { esp_task_wdt_reset(); vTaskDelay(pdMS_TO_TICKS(20)); }
    if ((millis() - t0) >= 3000) {                               // long press -> WDT demo
      Serial.println("[BTN] long press -> watchdog demo");
      wdtHangRequested = true;
      while (digitalRead(BTN_PIN) == LOW) { esp_task_wdt_reset(); vTaskDelay(pdMS_TO_TICKS(50)); }
    } else {                                                     // short press -> diagnostics
      Serial.println("[BTN] short press -> diagnostics screen");
      DisplayMsg dm; memset(&dm, 0, sizeof(dm)); dm.kind = DM_DIAG;
      xQueueSend(qDisplay, &dm, 0);
    }
  }
}

// ============================== ESP-NOW (A4) ==============================
static void espnowRecv(ESPNOW_RECV_SIG) {
  if (len != (int)sizeof(FpMsg)) return;
  const FpMsg *m = (const FpMsg *)data;
  if (m->magic != FP_MAGIC) return;
  if (!peerConfigured || memcmp(ESPNOW_SRC_MAC, peerMac, 6) != 0) return;
  lastPeerRxMs = millis();
  if (m->type == MSG_ACK) { ackSeq = m->seq; xSemaphoreGive(semAck); return; }
  xQueueSend(qEspRx, m, 0);
}

static void initEspNow() {
  WiFi.mode(WIFI_STA);
  if (esp_now_init() != ESP_OK) { Serial.println("[ESP-NOW] init failed"); return; }
  esp_now_register_recv_cb(espnowRecv);
  peerConfigured = parseMac(cfg.peerMac, peerMac);
  if (peerConfigured) {
    esp_now_peer_info_t p; memset(&p, 0, sizeof(p));
    memcpy(p.peer_addr, peerMac, 6);
    p.channel = 0;                                   // 0 = follow the current (router) channel
    p.encrypt = false;
    p.ifidx   = WIFI_IF_STA;
    if (esp_now_add_peer(&p) != ESP_OK) { Serial.println("[ESP-NOW] add_peer failed"); peerConfigured = false; }
  }
  Serial.printf("[ESP-NOW] peer %s (%s)\n", cfg.peerMac, peerConfigured ? "configured" : "NOT configured");
}

static bool sendReliable(FpMsg &m, uint8_t tries, uint32_t waitMs) {
  for (uint8_t t = 0; t < tries; t++) {
    xSemaphoreTake(semAck, 0);                       // clear stale give
    if (esp_now_send(peerMac, (uint8_t *)&m, sizeof(m)) != ESP_OK) { vTaskDelay(pdMS_TO_TICKS(20)); continue; }
    uint32_t t0 = millis();
    while ((millis() - t0) < waitMs) {
      if (xSemaphoreTake(semAck, pdMS_TO_TICKS(10)) == pdTRUE && ackSeq == m.seq) return true;
    }
  }
  return false;
}

static void CommTask(void *) {
  esp_task_wdt_add(NULL);
  CommItem it;
  FpMsg rx;
  for (;;) {
    esp_task_wdt_reset();
    bool alive = lastPeerRxMs && ((millis() - lastPeerRxMs) < 5000);
    if (alive) xEventGroupSetBits(evGroup, EVT_PEER_LINK); else xEventGroupClearBits(evGroup, EVT_PEER_LINK);

    if (!peerConfigured) {                           // no node 2 configured: drop, keep running
      while (xQueueReceive(qComm, &it, 0) == pdTRUE) {}
      vTaskDelay(pdMS_TO_TICKS(200));
      continue;
    }
    if (xQueueReceive(qComm, &it, pdMS_TO_TICKS(20)) == pdTRUE) {
      FpMsg m; memset(&m, 0, sizeof(m));
      m.magic = FP_MAGIC; m.type = it.type; m.seq = ++txSeq; m.value = it.value;
      m.uptimeS = millis() / 1000; m.studentId = it.id;
      strlcpy(m.name, it.name, sizeof(m.name));
      strlcpy(m.reg, it.reg, sizeof(m.reg));
      bool hb = (it.type == MSG_HEARTBEAT);
      bool ok = sendReliable(m, hb ? 1 : 3, hb ? 60 : 100);   // events: 3 tries, heartbeat: 1
      if (!ok && !hb) Serial.printf("[ESP-NOW] no ACK for type %u\n", it.type);
    }
    while (xQueueReceive(qEspRx, &rx, 0) == pdTRUE) {         // messages from node 2
      if (rx.type == MSG_CMD) {
        FpCmd c; memset(&c, 0, sizeof(c));
        if (rx.value == CMD_PAUSE)       c.kind = FC_PAUSE;
        else if (rx.value == CMD_RESUME) c.kind = FC_RESUME;
        else if (rx.value == CMD_WDT_TEST) { wdtHangRequested = true; Serial.println("[ESP-NOW] WDT test requested by node 2"); }
        if (c.kind) { xQueueSend(qFpCmd, &c, 0); if (hFinger) xTaskNotifyGive(hFinger); }
      }
      FpMsg ack; memset(&ack, 0, sizeof(ack));
      ack.magic = FP_MAGIC; ack.type = MSG_ACK; ack.seq = rx.seq; ack.uptimeS = millis() / 1000;
      esp_now_send(peerMac, (uint8_t *)&ack, sizeof(ack));
    }
  }
}

// ============================== MQTT + NETWORK ==============================
static void mqttCallback(char *topic, byte *payload, unsigned int length) {
  char buf[MQTT_BUF_SIZE];
  unsigned int maxLen = MQTT_BUF_SIZE - 1;
  unsigned int len = (length < maxLen) ? length : maxLen;
  memcpy(buf, payload, len);
  buf[len] = '\0';
  Serial.printf("[MQTT] <- %s : %s\n", topic, buf);

  FpCmd c; memset(&c, 0, sizeof(c));
  if (strcmp(topic, TOPIC_SYS_STATE) == 0) {
    c.kind = FC_STATE;
    strlcpy(c.text, buf, sizeof(c.text));
    xQueueSend(qFpCmd, &c, 0);
    if (hFinger) xTaskNotifyGive(hFinger);
    return;
  }
  if (strcmp(topic, TOPIC_ENROLL_DATA) == 0) {
    StaticJsonDocument<256> doc;
    if (deserializeJson(doc, buf) == DeserializationError::Ok) {
      c.kind = FC_ENROLL_DATA;
      strlcpy(c.name, doc["name"] | "", sizeof(c.name));
      strlcpy(c.reg,  doc["regNum"] | "", sizeof(c.reg));
      xQueueSend(qFpCmd, &c, 0);
      if (hFinger) xTaskNotifyGive(hFinger);
      Serial.printf("[MQTT] Enroll parsed: %s / %s\n", c.name, c.reg);
    } else Serial.println("[MQTT] fp/enrollData parse FAILED");
  }
}

static bool mqttPublishNow(const char *topic, const char *payload, bool retained) {
  if (!mqttClient.connected()) return false;
  bool ok = mqttClient.publish(topic, payload, retained);
  Serial.printf("[MQTT] %s -> %s\n", ok ? "PUB" : "FAIL", topic);
  return ok;
}

static bool publishAttendance(const Attendance &r) {
  StaticJsonDocument<300> doc;
  doc["id"] = r.id; doc["name"] = r.name; doc["regNum"] = r.regNum;
  doc["timestamp"] = r.timestamp; doc["ntpSynced"] = true;
  char buf[320];
  serializeJson(doc, buf, sizeof(buf));
  return mqttPublishNow(TOPIC_STR[T_ATTENDANCE], buf, false);
}

static void mqttReconnect() {
  char cid[64];
  snprintf(cid, sizeof(cid), "%s_%08X", cfg.deviceName, (unsigned)(ESP.getEfuseMac() & 0xFFFFFFFF));
  Serial.printf("[MQTT] Connecting as %s...\n", cid);
  bool ok = cfg.mqttUser[0] ? mqttClient.connect(cid, cfg.mqttUser, cfg.mqttPass) : mqttClient.connect(cid);
  if (ok) {
    mqttClient.subscribe(TOPIC_SYS_STATE, 1);
    mqttClient.subscribe(TOPIC_ENROLL_DATA, 1);
    Serial.println("[MQTT] Connected & subscribed");
    mqttPublishNow(TOPIC_STR[T_STATE_PUB], "VERIFY", true);
    mqttPublishNow(TOPIC_STR[T_MESSAGE], "ESP32 online", false);
  } else {
    Serial.printf("[MQTT] Failed, state=%d\n", mqttClient.state());
  }
}

static void handleAttendance(const Attendance &rec) {
  bool tsValid = strncmp(rec.timestamp, "1970", 4) != 0;
  const EventBits_t need = EVT_MQTT_UP | EVT_NTP_OK;
  bool online = (xEventGroupGetBits(evGroup) & need) == need;
  if (tsValid && online && publishAttendance(rec)) return;

  StoreOp op; memset(&op, 0, sizeof(op));
  op.op = OP_ADD_OFFLINE; op.rec = rec;
  xQueueSend(qStore, &op, pdMS_TO_TICKS(100));
  Serial.println("[Verify] Stored offline");
  if (tsValid) {
    bool mq = (xEventGroupGetBits(evGroup) & EVT_MQTT_UP) != 0;
    uiBottom(mq ? "Saved offline!" : "Offline stored!", 20);
    commSend(MSG_ATT_EVENT, RES_OFFLINE_SAVED, rec.id, rec.name, rec.regNum);
  }
}

static void NetTask(void *) {
  esp_task_wdt_add(NULL);
  wifiSecure.setInsecure();
  wifiSecure.setTimeout(4);
  mqttClient.setServer(cfg.mqttBroker, cfg.mqttPort);
  mqttClient.setCallback(mqttCallback);
  mqttClient.setBufferSize(MQTT_BUF_SIZE);
  mqttClient.setKeepAlive(30);
  mqttClient.setSocketTimeout(4);

  WiFi.setAutoReconnect(true);
  WiFi.begin(cfg.wifiSsid, cfg.wifiPass);
  uint32_t lastWifiTry = millis(), lastMqttTry = 0, lastSync = 0;
  bool wasWifi = false, syncing = false;

  for (;;) {
    esp_task_wdt_reset();
    bool wifiOk = (WiFi.status() == WL_CONNECTED);
    if (wifiOk) {
      xEventGroupSetBits(evGroup, EVT_WIFI_UP);
      if (!wasWifi) {
        wasWifi = true;
        Serial.printf("[WiFi] Connected %s ch=%d\n", WiFi.localIP().toString().c_str(), WiFi.channel());
        configTime(gmtOffset_sec, daylightOffset_sec, ntpServer, ntpServer2);
      }
    } else {
      xEventGroupClearBits(evGroup, EVT_WIFI_UP | EVT_MQTT_UP);
      wasWifi = false;
      if (millis() - lastWifiTry > 20000) {
        lastWifiTry = millis();
        Serial.println("[WiFi] Retrying...");
        WiFi.disconnect();
        WiFi.begin(cfg.wifiSsid, cfg.wifiPass);
      }
    }

    if (xEventGroupGetBits(evGroup) & EVT_NTP_REQ) {             // hourly resync request from HeartbeatTask
      xEventGroupClearBits(evGroup, EVT_NTP_REQ);
      if (wifiOk) { Serial.println("[NTP] Resync"); configTime(gmtOffset_sec, daylightOffset_sec, ntpServer, ntpServer2); }
    }

    if (wifiOk) {
      if (!mqttClient.connected()) {
        xEventGroupClearBits(evGroup, EVT_MQTT_UP);
        if (millis() - lastMqttTry > 3000) { lastMqttTry = millis(); mqttReconnect(); }
      } else {
        xEventGroupSetBits(evGroup, EVT_MQTT_UP);
        mqttClient.loop();
      }
    }

    MqttOut o; int n = 0;
    while (n++ < 4 && xQueueReceive(qMqttOut, &o, 0) == pdTRUE) mqttPublishNow(TOPIC_STR[o.topic], o.payload, o.retained);

    Attendance rec;
    if (xQueueReceive(qAtt, &rec, 0) == pdTRUE) handleAttendance(rec);

    // ---- offline sync (one record per 400 ms) ----
    const EventBits_t need = EVT_MQTT_UP | EVT_NTP_OK;
    bool online = (xEventGroupGetBits(evGroup) & need) == need;
    if (online && offlineCount > 0 && (millis() - lastSync) > 400) {
      lastSync = millis();
      Attendance r; int idx = -1;
      if (xSemaphoreTake(mtxData, pdMS_TO_TICKS(50)) == pdTRUE) {
        for (int i = 0; i < offlineCount; i++) {
          if (strncmp(offlineAttendance[i].timestamp, "1970", 4) != 0) { r = offlineAttendance[i]; idx = i; break; }
        }
        xSemaphoreGive(mtxData);
      }
      if (idx >= 0) {
        if (!syncing) { syncing = true; uiBottom("Syncing offline...", 0); }
        if (publishAttendance(r)) {
          StoreOp op; memset(&op, 0, sizeof(op));
          op.op = OP_REMOVE_AT; op.idx = (uint8_t)idx;
          xQueueSend(qStore, &op, pdMS_TO_TICKS(100));
        } else uiBottom("Sync failed! Retry later...", 30);
      }
    }
    if (syncing && (offlineCount == 0 || !online)) {
      syncing = false;
      if (offlineCount == 0) uiBottom("Sync complete!", 20); else uiIdle();
    }
    vTaskDelay(pdMS_TO_TICKS(20));
  }
}

// ============================== FINGERPRINT ==============================
static void showStateLabel() {
  uiState(scanPaused ? "PAUSED" : (currentState == ENROLL ? "ENROLL" : "VERIFY"));
}

static void waitFingerRemoved(uint32_t maxMs) {
  uint32_t t0 = millis();
  while ((millis() - t0) < maxMs) {
    esp_task_wdt_reset();
    if (finger.getImage() == FINGERPRINT_NOFINGER) break;
    vTaskDelay(pdMS_TO_TICKS(100));
  }
}

static bool waitForImage(uint32_t timeoutMs) {
  uint32_t start = millis();
  int p;
  uiProgress(0);
  while ((p = finger.getImage()) != FINGERPRINT_OK) {
    esp_task_wdt_reset();
    if (p != FINGERPRINT_NOFINGER) uiBottom("Image error!", 5);
    uint32_t pct = ((millis() - start) * 100UL) / timeoutMs;
    if (pct > 100) pct = 100;
    uiProgress((uint8_t)pct);
    if ((millis() - start) > timeoutMs) { uiBottom("Enroll timeout", 0, true); uiProgress(0); return false; }
    vTaskDelay(pdMS_TO_TICKS(80));
  }
  uiProgress(0);
  return true;
}

static bool enrollFinger(const char *nameIn, const char *regIn) {
  String name(nameIn), regNum(regIn);
  xSemaphoreTake(mtxData, portMAX_DELAY);
  uint8_t cnt = studentCount;
  xSemaphoreGive(mtxData);

  if (cnt >= MAX_STUDENTS) { uiBottom("Max students!", 0, true); ledPulse(RED_LED, 1000); return false; }
  if (name.length() == 0 || regNum.length() == 0) { uiBottom("Invalid data!", 0, true); ledPulse(RED_LED, 1000); return false; }

  uint8_t id = cnt + 1;
  uiBottom("Place finger to enroll...", 0, true);
  if (!waitForImage(15000)) return false;

  if (finger.image2Tz(1) != FINGERPRINT_OK) { uiBottom("Image fail", 0, true); ledPulse(RED_LED, 1000); return false; }
  if (finger.fingerSearch() == FINGERPRINT_OK) { uiBottom("Already Enrolled!", 0, true); ledPulse(RED_LED, 1000); return false; }

  uiBottom("Remove finger", 0, true);
  vTaskDelay(pdMS_TO_TICKS(800));
  waitFingerRemoved(10000);

  uiBottom("Place again...", 0, true);
  if (!waitForImage(15000)) return false;

  if (finger.image2Tz(2) != FINGERPRINT_OK)  { uiBottom("2nd fail", 0, true);   ledPulse(RED_LED, 900); return false; }
  if (finger.createModel() != FINGERPRINT_OK) { uiBottom("Model fail", 0, true); ledPulse(RED_LED, 900); return false; }
  if (finger.storeModel(id) != FINGERPRINT_OK) { uiBottom("Store fail", 0, true); ledPulse(RED_LED, 900); return false; }

  xSemaphoreTake(mtxData, portMAX_DELAY);
  students[studentCount].id = id;
  memset(students[studentCount].name, 0, STUDENT_NAME_LEN);
  memset(students[studentCount].regNum, 0, STUDENT_REG_LEN);
  name.toCharArray(students[studentCount].name, STUDENT_NAME_LEN);
  regNum.toCharArray(students[studentCount].regNum, STUDENT_REG_LEN);
  studentCount++;
  xSemaphoreGive(mtxData);
  StoreOp op; memset(&op, 0, sizeof(op)); op.op = OP_SAVE_STUDENTS;
  xQueueSend(qStore, &op, pdMS_TO_TICKS(200));

  char ts[TS_LEN];
  StaticJsonDocument<256> doc;
  doc["id"] = id; doc["name"] = name; doc["regNum"] = regNum;
  doc["enrolledAt"] = getTimestampStr(ts, sizeof(ts)) ? ts : "";
  char out[300];
  serializeJson(doc, out, sizeof(out));
  mqttEnqueue(T_ENROLLED, out);

  String okMsg = "Enroll Success: " + name;
  uiBottom(okMsg.c_str(), 0, true);
  Serial.printf("[Enroll] OK id=%d name=%s\n", id, name.c_str());
  ledPulse(GREEN_LED, 900);
  commSend(MSG_ATT_EVENT, RES_ENROLL_OK, id, name.c_str(), regNum.c_str());
  return true;
}

static void runEnrollment() {
  uiBottom("Enrollment Started...", 0, true);
  Serial.println("[Enroll] Waiting for fp/enrollData...");
  FpCmd c; memset(&c, 0, sizeof(c));
  bool got = false;
  uint32_t start = millis();
  while (!got && (millis() - start) < 12000) {
    esp_task_wdt_reset();
    if (havePending) { c = pendingEnroll; havePending = false; got = true; break; }   // data arrived with the state
    FpCmd q;
    if (xQueueReceive(qFpCmd, &q, pdMS_TO_TICKS(100)) == pdTRUE) {
      if (q.kind == FC_ENROLL_DATA) { c = q; got = true; }
      else if (q.kind == FC_STATE && strcmp(q.text, "ENROLL") != 0) {                  // cancelled from dashboard
        currentState = VERIFY; havePending = false; showStateLabel(); uiIdle(); return;
      }
    }
  }
  if (got) {
    Serial.printf("[Enroll] Data: name=%s regNum=%s\n", c.name, c.reg);
    if (enrollFinger(c.name, c.reg)) { uiBottom("Enrollment Complete!", 10, true); vTaskDelay(pdMS_TO_TICKS(1000)); }
    else commSend(MSG_ATT_EVENT, RES_ENROLL_FAIL);
  } else {
    Serial.println("[Enroll] Timeout, no fp/enrollData");
    uiBottom("No enroll data!", 15, true);
    ledPulse(RED_LED, 1500);
    commSend(MSG_ATT_EVENT, RES_ENROLL_FAIL);
    vTaskDelay(pdMS_TO_TICKS(1500));
  }
  currentState = VERIFY;
  havePending = false;
  mqttEnqueue(T_STATE_PUB, "VERIFY", true);
  showStateLabel();
  uiIdle();
}

static void verifyOnce() {
  statScans = statScans + 1;
  int p = finger.getImage();
  if (p == FINGERPRINT_NOFINGER) return;
  if (p != FINGERPRINT_OK)                  { uiBottom("Image error!", 8);    return; }
  if (finger.image2Tz(1) != FINGERPRINT_OK) { uiBottom("Image conv fail", 8); return; }

  uint16_t hu = cfg.welcomeHoldMs / 100;
  uint8_t holdUnits = (hu > 250) ? 250 : (uint8_t)hu;

  if (finger.fingerFastSearch() == FINGERPRINT_OK) {
    uint8_t id = finger.fingerID;
    char name[STUDENT_NAME_LEN], reg[STUDENT_REG_LEN];
    lookupStudent(id, name, reg);

    char ts[TS_LEN];
    bool timeOk = getTimestampStr(ts, sizeof(ts));
    char msg[48];
    if (timeOk) snprintf(msg, sizeof(msg), "Welcome:\n-> %s", name);
    else        strlcpy(msg, "No time sync!\nStored offline.", sizeof(msg));
    uiBottom(msg, holdUnits);
    ledPulse(GREEN_LED, 180);
    Serial.printf("[Verify] id=%d name=%s ts=%s synced=%d\n", id, name, timeOk ? ts : "-", (int)timeOk);

    Attendance rec; memset(&rec, 0, sizeof(rec));
    rec.id = id;
    strlcpy(rec.name, name, sizeof(rec.name));
    strlcpy(rec.regNum, reg, sizeof(rec.regNum));
    if (timeOk) strlcpy(rec.timestamp, ts, sizeof(rec.timestamp));
    else        strlcpy(rec.timestamp, "1970-01-01T00:00:00+05:30", sizeof(rec.timestamp));

    if (xQueueSend(qAtt, &rec, pdMS_TO_TICKS(100)) != pdTRUE) {            // NetTask busy -> store directly
      StoreOp op; memset(&op, 0, sizeof(op)); op.op = OP_ADD_OFFLINE; op.rec = rec;
      xQueueSend(qStore, &op, pdMS_TO_TICKS(100));
    }
    commSend(MSG_ATT_EVENT, RES_MATCH, id, name, reg);

    delayFeed(cfg.welcomeHoldMs);
    waitFingerRemoved(5000);
  } else {
    uiBottom("No Match!", 10);
    ledPulse(RED_LED, 150);
    commSend(MSG_ATT_EVENT, RES_NOMATCH);
    delayFeed(1000);
    waitFingerRemoved(5000);
  }
}

static void handleCmd(const FpCmd &c) {
  switch (c.kind) {
    case FC_STATE:
      if (strcmp(c.text, "ENROLL") == 0) { currentState = ENROLL; scanPaused = false; }
      else { currentState = VERIFY; havePending = false; uiIdle(); }
      Serial.printf("[State] -> %s\n", c.text);
      showStateLabel();
      break;
    case FC_ENROLL_DATA:                              // keep it: it usually arrives right after the ENROLL state
      pendingEnroll = c;
      havePending = true;
      break;
    case FC_PAUSE:
      scanPaused = true; showStateLabel();
      uiBottom("Paused by Node 2", 0);
      Serial.println("[Cmd] PAUSE");
      break;
    case FC_RESUME:
      scanPaused = false; showStateLabel(); uiIdle();
      Serial.println("[Cmd] RESUME");
      break;
    default: break;
  }
}

static void FingerprintTask(void *) {
  esp_task_wdt_add(NULL);
  showStateLabel();
  uiIdle();
  for (;;) {
    esp_task_wdt_reset();
    FpCmd c;
    while (xQueueReceive(qFpCmd, &c, 0) == pdTRUE) handleCmd(c);
    if (scanPaused) { ulTaskNotifyTake(pdTRUE, pdMS_TO_TICKS(500)); continue; }
    if (currentState == ENROLL) { runEnrollment(); continue; }

    // WAKEUP pin mode : sleep until the touch interrupt (1 s timeout only as safety net)
    // polling mode    : sleep scanPollMs between sensor checks (task is blocked, not busy-waiting)
    ulTaskNotifyTake(pdTRUE, pdMS_TO_TICKS(cfg.useWakePin ? 1000 : cfg.scanPollMs));
    esp_task_wdt_reset();
    if (uxQueueMessagesWaiting(qFpCmd)) continue;      // woken by a command
    if (scanPaused || currentState != VERIFY) continue;
    verifyOnce();
  }
}

// ============================== CONFIG PORTAL (A2) ==============================
// Replace the whole old "static const char PORTAL_HTML[] PROGMEM = R"rawliteral( ... )rawliteral";"
// block in the station sketch with this one. The WebSocket protocol is unchanged
// (get / save / reboot / wdtTest  <->  config / status / saved / error / info).

static const char PORTAL_HTML[] PROGMEM = R"rawliteral(
<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Attendance Station - Setup</title>
<style>
:root{--bg:#f3f5f8;--panel:#fff;--line:#d8dee6;--ink:#17212b;--mute:#5a6776;--acc:#0b6e8a;--acc2:#e6f2f6;--ok:#17703f;--bad:#b3261e;--warn:#9a6700}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.5 system-ui,-apple-system,"Segoe UI",Roboto,Arial,sans-serif}
.wrap{max-width:720px;margin:0 auto;padding:18px 14px 90px}
header{display:flex;justify-content:space-between;align-items:flex-start;gap:12px;flex-wrap:wrap;margin-bottom:12px}
h1{font-size:22px;margin:0;font-weight:700;letter-spacing:-.01em}
.sub{color:var(--mute);font-size:13px;margin-top:2px}
.pill{display:inline-flex;align-items:center;gap:7px;padding:5px 12px;border-radius:999px;border:1px solid var(--line);background:var(--panel);font-size:13px;font-weight:600}
.dot{width:9px;height:9px;border-radius:50%;background:var(--warn)}
.pill.ok .dot{background:var(--ok)}.pill.ok{color:var(--ok)}
.pill.bad .dot{background:var(--bad)}.pill.bad{color:var(--bad)}
.stats{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:14px}
.stat{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:6px 11px;font-size:12.5px;color:var(--mute)}
.stat b{color:var(--ink);font-weight:600;margin-left:4px;font-family:ui-monospace,Consolas,Menlo,monospace}
section{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:14px 16px 16px;margin-bottom:12px}
section h2{font-size:15px;margin:0 0 2px}
section p{margin:0 0 8px;color:var(--mute);font-size:13px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:0 14px}
@media(max-width:560px){.grid{grid-template-columns:1fr}}
label{display:block;margin:10px 0 4px;font-size:13px;font-weight:600}
.hint{font-weight:400;color:var(--mute)}
input,select{width:100%;padding:9px 10px;border:1px solid #bcc5d0;border-radius:7px;background:#fff;color:var(--ink);font:inherit}
input:focus,select:focus,button:focus-visible{outline:2px solid var(--acc);outline-offset:1px;border-color:var(--acc)}
.pw{position:relative}.pw input{padding-right:62px}
.pw button{position:absolute;right:4px;top:4px;bottom:4px;padding:0 10px;margin:0;background:var(--acc2);color:var(--acc);border:0;border-radius:5px;font-size:12px;font-weight:600;cursor:pointer}
.bar{position:fixed;left:0;right:0;bottom:0;background:rgba(255,255,255,.96);border-top:1px solid var(--line);padding:10px 14px}
.bar div{max-width:720px;margin:0 auto;display:flex;gap:8px;flex-wrap:wrap}
button.b{padding:10px 16px;border-radius:7px;border:1px solid var(--line);background:#fff;color:var(--ink);font:inherit;font-weight:600;cursor:pointer}
button.b.p{background:var(--acc);border-color:var(--acc);color:#fff}
button.b.d{color:var(--bad);border-color:#e7b9b5}
button.b:active{transform:translateY(1px)}
.sp{flex:1}
#term{margin-top:4px}
#term h2{display:flex;justify-content:space-between;align-items:center}
#term h2 button{font-size:12px;border:0;background:none;color:var(--acc);cursor:pointer;font-weight:600}
#con{background:#f7f8fa;border:1px solid var(--line);border-radius:7px;height:170px;overflow:auto;padding:8px 10px;font:12.5px/1.55 ui-monospace,Consolas,Menlo,monospace}
#con .t{color:#8a95a3}#con .s{color:var(--acc)}#con .r{color:var(--ink)}#con .e{color:var(--bad)}#con .g{color:var(--ok)}
#toast{position:fixed;left:50%;bottom:78px;transform:translateX(-50%);max-width:92%;padding:10px 16px;border-radius:8px;background:var(--ink);color:#fff;font-size:14px;display:none;z-index:5}
#toast.ok{background:var(--ok)}#toast.bad{background:var(--bad)}
#ov{position:fixed;inset:0;background:rgba(243,245,248,.97);display:none;align-items:center;justify-content:center;padding:24px;z-index:9;text-align:center}
#ov div{max-width:380px}#ov h2{margin:0 0 8px;font-size:20px}#ov p{color:var(--mute);margin:6px 0}
</style></head><body>
<div class="wrap">
<header>
  <div><h1>Attendance Station</h1><div class="sub">Device setup. Changes are stored on the board and survive power loss.</div></div>
  <span id="pill" class="pill"><span class="dot"></span><span id="pillt">Connecting</span></span>
</header>
<div class="stats">
  <div class="stat">Board MAC<b id="sMac">-</b></div>
  <div class="stat">Uptime<b id="sUp">-</b></div>
  <div class="stat">Free heap<b id="sHeap">-</b></div>
  <div class="stat">Browsers<b id="sCli">-</b></div>
</div>

<section>
  <h2>Device</h2>
  <p>Name shown to the network and the MAC of the second board that mirrors attendance feedback.</p>
  <div class="grid">
    <div><label for="deviceName">Device name</label><input id="deviceName" maxlength="23" autocomplete="off"></div>
    <div><label for="peerMac">Aux node (ESP32-C3) MAC <span class="hint">optional</span></label><input id="peerMac" placeholder="AA:BB:CC:DD:EE:FF" autocomplete="off"></div>
  </div>
</section>

<section>
  <h2>Wi-Fi</h2>
  <p>The network the station joins in normal mode.</p>
  <div class="grid">
    <div><label for="wifiSsid">Network name (SSID)</label><input id="wifiSsid" maxlength="32" autocomplete="off"></div>
    <div><label for="wifiPass">Password</label><div class="pw"><input id="wifiPass" type="password" maxlength="64" autocomplete="new-password"><button type="button" onclick="eye('wifiPass',this)">Show</button></div></div>
  </div>
</section>

<section>
  <h2>MQTT broker</h2>
  <p>Where attendance events are published (TLS, usually port 8883).</p>
  <div class="grid">
    <div><label for="mqttBroker">Broker host</label><input id="mqttBroker" maxlength="95" autocomplete="off"></div>
    <div><label for="mqttPort">Port</label><input id="mqttPort" type="number" min="1" max="65535"></div>
    <div><label for="mqttUser">Username</label><input id="mqttUser" maxlength="47" autocomplete="off"></div>
    <div><label for="mqttPass">Password</label><div class="pw"><input id="mqttPass" type="password" maxlength="63" autocomplete="new-password"><button type="button" onclick="eye('mqttPass',this)">Show</button></div></div>
  </div>
</section>

<section>
  <h2>Timing and sensor</h2>
  <p>Leave a password field empty to keep the stored one.</p>
  <div class="grid">
    <div><label for="hbIntervalSec">Cloud heartbeat <span class="hint">seconds, 1-3600</span></label><input id="hbIntervalSec" type="number" min="1" max="3600"></div>
    <div><label for="welcomeHoldMs">Welcome message hold <span class="hint">ms, 500-10000</span></label><input id="welcomeHoldMs" type="number" min="500" max="10000"></div>
    <div><label for="scanPollMs">Finger poll interval <span class="hint">ms, 100-2000</span></label><input id="scanPollMs" type="number" min="100" max="2000"></div>
    <div><label for="useWakePin">Finger detection</label>
      <select id="useWakePin"><option value="0">Timed polling</option><option value="1">WAKEUP pin interrupt (GPIO27)</option></select></div>
  </div>
</section>

<section id="term">
  <h2>Console <button type="button" onclick="clr()">Clear</button></h2>
  <div id="con"></div>
</section>
</div>

<div class="bar"><div>
  <button class="b p" onclick="save(false)">Save</button>
  <button class="b" onclick="save(true)">Save and reboot</button>
  <span class="sp"></span>
  <button class="b" onclick="reboot()">Reboot</button>
  <button class="b d" onclick="wdt()">Watchdog test</button>
</div></div>

<div id="toast"></div>
<div id="ov"><div><h2 id="ovt"></h2><p id="ovp"></p></div></div>

<script>
var ws=null,closing=false,fails=0;
var F=['deviceName','peerMac','wifiSsid','wifiPass','mqttBroker','mqttPort','mqttUser','mqttPass','hbIntervalSec','welcomeHoldMs','scanPollMs','useWakePin'];
var NUM={mqttPort:1,hbIntervalSec:1,welcomeHoldMs:1,scanPollMs:1,useWakePin:1};
var RANGE={mqttPort:[1,65535,'MQTT port'],hbIntervalSec:[1,3600,'Heartbeat'],welcomeHoldMs:[500,10000,'Welcome hold'],scanPollMs:[100,2000,'Poll interval']};
function $(i){return document.getElementById(i)}
function pad(n){return n<10?'0'+n:''+n}
function log(cls,txt,tag){
  var d=new Date(),c=$('con'),l=document.createElement('div');
  l.innerHTML='<span class="t">'+pad(d.getHours())+':'+pad(d.getMinutes())+':'+pad(d.getSeconds())+'</span> <span class="'+cls+'"></span>';
  l.lastChild.textContent=(tag||'')+' '+txt;
  c.appendChild(l);
  while(c.childNodes.length>200)c.removeChild(c.firstChild);
  c.scrollTop=c.scrollHeight;
}
function clr(){$('con').innerHTML=''}
function toast(t,c){var e=$('toast');e.textContent=t;e.className=c||'';e.style.display='block';clearTimeout(toast.h);toast.h=setTimeout(function(){e.style.display='none'},3500)}
function pill(t,c){$('pillt').textContent=t;$('pill').className='pill '+(c||'')}
function eye(id,b){var i=$(id);var s=i.type=='password';i.type=s?'text':'password';b.textContent=s?'Hide':'Show'}
function closed(t,p){closing=true;$('ovt').textContent=t;$('ovp').textContent=p;$('ov').style.display='flex';pill('Portal closed','bad')}
function redact(o){var c={};for(var k in o){c[k]=((k=='wifiPass'||k=='mqttPass')&&o[k])?'***':o[k]}return c}
function send(o){
  if(!ws||ws.readyState!=1){toast('Not connected to the station','bad');return false}
  ws.send(JSON.stringify(o));log('s',JSON.stringify(redact(o)),'>');return true;
}
function save(r){
  var o={cmd:'save',reboot:r};
  for(var i=0;i<F.length;i++){
    var k=F[i],v=$(k).value;
    if(NUM[k]){v=parseInt(v,10);if(isNaN(v)){toast('Enter a number for '+k,'bad');return}
      var g=RANGE[k];if(g&&(v<g[0]||v>g[1])){toast(g[2]+' must be '+g[0]+' to '+g[1],'bad');return}}
    o[k]=v;
  }
  if(!o.wifiSsid){toast('Wi-Fi name is required','bad');return}
  if(!o.mqttBroker){toast('MQTT broker host is required','bad');return}
  send(o);
}
function reboot(){if(confirm('Reboot the station now? It will start in normal mode.'))send({cmd:'reboot'})}
function wdt(){if(confirm('This hangs a task on purpose so the watchdog resets the board. Continue?'))send({cmd:'wdtTest'})}
function fill(m){
  for(var i=0;i<F.length;i++){var k=F[i];if(k!='wifiPass'&&k!='mqttPass'&&m[k]!==undefined)$(k).value=m[k]}
  $('wifiPass').value='';$('mqttPass').value='';
  $('wifiPass').placeholder=m.wifiPassSet?'Stored - leave empty to keep':'Not set';
  $('mqttPass').placeholder=m.mqttPassSet?'Stored - leave empty to keep':'Not set';
  $('sMac').textContent=m.myMac||'-';
}
function conn(){
  if(closing)return;
  pill('Connecting','');
  ws=new WebSocket('ws://'+location.hostname+':81/');
  ws.onopen=function(){fails=0;pill('Connected','ok');log('g','WebSocket connected','*');send({cmd:'get'})};
  ws.onclose=function(){
    if(closing)return;
    fails++;pill('Reconnecting','bad');
    if(fails==4)log('e','Station not reachable. If you saved with reboot, the portal is closed: hold the button and press reset to open it again.','!');
    setTimeout(conn,1500);
  };
  ws.onmessage=function(e){
    var m;try{m=JSON.parse(e.data)}catch(x){return}
    if(m.type=='status'){
      $('sMac').textContent=m.myMac||'-';$('sUp').textContent=m.uptime+' s';
      $('sHeap').textContent=Math.round(m.heap/1024)+' kB';$('sCli').textContent=m.clients;return;
    }
    if(m.type=='config'){fill(m);log('r','configuration loaded','<');return}
    if(m.type=='saved'){
      log('g',m.msg,'<');toast(m.msg,'ok');
      if(m.msg.indexOf('Rebooting')>=0)closed('Saved - station is rebooting','It restarts in normal mode and the setup network closes. Connect back to your normal Wi-Fi. To open this page again, hold the button while pressing reset.');
      return;
    }
    if(m.type=='error'){log('e',m.msg,'<');toast(m.msg,'bad');return}
    if(m.type=='info'){
      log('r',m.msg,'<');toast(m.msg,'');
      if(m.msg.indexOf('Rebooting')>=0)closed('Station is rebooting','It restarts in normal mode and the setup network closes.');
      else if(m.msg.toLowerCase().indexOf('watchdog')>=0)closed('Watchdog test running','A task is hung on purpose. The watchdog resets the board in about 10 seconds and it restarts in normal mode.');
    }
  };
}
conn();
</script></body></html>
)rawliteral";


static WebServer        http(80);
static WebSocketsServer wsServer(81);
static volatile bool    cfgHang = false;
static uint32_t         rebootAtMs = 0;

static void wsReply(uint8_t num, const char *type, const char *msg) {
  StaticJsonDocument<256> d;
  d["type"] = type; d["msg"] = msg;
  String s; serializeJson(d, s);
  wsServer.sendTXT(num, s);
}

static void wsSendConfig(uint8_t num) {
  StaticJsonDocument<1024> d;
  d["type"] = "config";
  d["deviceName"] = cfg.deviceName;
  d["wifiSsid"] = cfg.wifiSsid;
  d["wifiPassSet"] = cfg.wifiPass[0] != 0;
  d["mqttBroker"] = cfg.mqttBroker;
  d["mqttPort"] = cfg.mqttPort;
  d["mqttUser"] = cfg.mqttUser;
  d["mqttPassSet"] = cfg.mqttPass[0] != 0;
  d["hbIntervalSec"] = cfg.hbIntervalSec;
  d["welcomeHoldMs"] = cfg.welcomeHoldMs;
  d["scanPollMs"] = cfg.scanPollMs;
  d["useWakePin"] = cfg.useWakePin;
  d["peerMac"] = cfg.peerMac;
  d["myMac"] = WiFi.macAddress();
  String s; serializeJson(d, s);
  wsServer.sendTXT(num, s);
}

static void wsHandleSave(uint8_t num, CfgDoc &d) {
  Config n = cfg;
  const char *s;

  s = d["deviceName"] | "";
  if (!*s || strlen(s) >= sizeof(n.deviceName)) { wsReply(num, "error", "Device name must be 1-23 chars"); return; }
  strlcpy(n.deviceName, s, sizeof(n.deviceName));

  s = d["wifiSsid"] | "";
  if (!*s || strlen(s) > 32) { wsReply(num, "error", "WiFi SSID must be 1-32 chars"); return; }
  strlcpy(n.wifiSsid, s, sizeof(n.wifiSsid));

  s = d["wifiPass"] | "";
  if (*s) {
    if (strlen(s) >= sizeof(n.wifiPass)) { wsReply(num, "error", "WiFi password too long"); return; }
    strlcpy(n.wifiPass, s, sizeof(n.wifiPass));
  }

  s = d["mqttBroker"] | "";
  if (!*s || strlen(s) >= sizeof(n.mqttBroker)) { wsReply(num, "error", "MQTT broker required"); return; }
  strlcpy(n.mqttBroker, s, sizeof(n.mqttBroker));

  int port = d["mqttPort"] | 0;
  if (port < 1 || port > 65535) { wsReply(num, "error", "MQTT port 1-65535"); return; }
  n.mqttPort = (uint16_t)port;

  s = d["mqttUser"] | "";
  if (strlen(s) >= sizeof(n.mqttUser)) { wsReply(num, "error", "MQTT user too long"); return; }
  strlcpy(n.mqttUser, s, sizeof(n.mqttUser));

  s = d["mqttPass"] | "";
  if (*s) {
    if (strlen(s) >= sizeof(n.mqttPass)) { wsReply(num, "error", "MQTT password too long"); return; }
    strlcpy(n.mqttPass, s, sizeof(n.mqttPass));
  }

  int hb = d["hbIntervalSec"] | 0;
  if (hb < 1 || hb > 3600) { wsReply(num, "error", "Heartbeat interval 1-3600 s"); return; }
  n.hbIntervalSec = (uint16_t)hb;

  int hold = d["welcomeHoldMs"] | 0;
  if (hold < 500 || hold > 10000) { wsReply(num, "error", "Welcome hold 500-10000 ms"); return; }
  n.welcomeHoldMs = (uint16_t)hold;

  int poll = d["scanPollMs"] | 0;
  if (poll < 100 || poll > 2000) { wsReply(num, "error", "Poll interval 100-2000 ms"); return; }
  n.scanPollMs = (uint16_t)poll;

  n.useWakePin = (d["useWakePin"] | 0) ? 1 : 0;

  s = d["peerMac"] | "";
  if (*s) {
    uint8_t tmp[6];
    if (!parseMac(s, tmp) || strlen(s) >= sizeof(n.peerMac)) { wsReply(num, "error", "Peer MAC format: AA:BB:CC:DD:EE:FF"); return; }
  }
  strlcpy(n.peerMac, s, sizeof(n.peerMac));

  saveConfig(n);
  cfg = n;
  bool reboot = d["reboot"] | false;
  if (reboot) { wsReply(num, "saved", "Saved to NVS. Rebooting..."); rebootAtMs = millis() + 1200; }
  else        { wsReply(num, "saved", "Saved to NVS (takes effect after reboot)"); }
}

static void wsEvent(uint8_t num, WStype_t type, uint8_t *payload, size_t length) {
  if (type == WStype_CONNECTED) { wsSendConfig(num); return; }
  if (type != WStype_TEXT) return;
  CfgDoc d;
  if (deserializeJson(d, (const char *)payload, length) != DeserializationError::Ok) { wsReply(num, "error", "Bad JSON"); return; }
  const char *cmd = d["cmd"] | "";
  Serial.printf("[WS] cmd=%s\n", cmd);
  if (!strcmp(cmd, "get"))            wsSendConfig(num);
  else if (!strcmp(cmd, "save"))      wsHandleSave(num, d);
  else if (!strcmp(cmd, "reboot"))  { wsReply(num, "info", "Rebooting..."); rebootAtMs = millis() + 800; }
  else if (!strcmp(cmd, "wdtTest")) { wsReply(num, "info", "ConfigTask will hang now - watchdog resets the board in ~10 s"); cfgHang = true; }
}

static void drawConfigScreen() {
  xSemaphoreTake(mtxI2C, portMAX_DELAY);
  display.clearDisplay();
  display.setTextSize(1);
  display.setTextColor(SH110X_WHITE);
  display.setCursor(0, 0);   display.println("== CONFIG MODE ==");
  display.setCursor(0, 20);  display.println("WiFi : FP-Config");
  display.setCursor(0, 32);  display.println("Pass : attendance");
  display.setCursor(0, 44);  display.println("Open : 192.168.4.1");
  display.setCursor(0, 64);  display.printf("Clients: %d\n", (int)wsServer.connectedClients());
  display.setCursor(0, 84);  display.println("Save & Reboot to run");
  display.display();
  xSemaphoreGive(mtxI2C);
}

static void ConfigTask(void *) {
  esp_task_wdt_add(NULL);
  uint32_t lastStatus = 0, lastOled = 0;
  for (;;) {
    esp_task_wdt_reset();
    http.handleClient();
    wsServer.loop();
    uint32_t now = millis();
    if (now - lastStatus > 2000) {                    // live push to every browser (real-time WebSocket data)
      lastStatus = now;
      StaticJsonDocument<256> d;
      d["type"] = "status"; d["uptime"] = now / 1000; d["heap"] = ESP.getFreeHeap();
      d["clients"] = wsServer.connectedClients(); d["myMac"] = WiFi.macAddress();
      String s; serializeJson(d, s);
      wsServer.broadcastTXT(s);
    }
    if (now - lastOled > 1000) { lastOled = now; drawConfigScreen(); }
    if (cfgHang) {
      Serial.println("[WDT-DEMO] ConfigTask blocked on purpose -> reset in ~10 s");
      for (;;) vTaskDelay(pdMS_TO_TICKS(1000));
    }
    if (rebootAtMs && (int32_t)(now - rebootAtMs) >= 0) ESP.restart();
    vTaskDelay(pdMS_TO_TICKS(2));
  }
}

static void startConfigMode() {
  Serial.println("[MODE] CONFIG MODE");
  digitalWrite(GREEN_LED, HIGH);
  WiFi.mode(WIFI_AP);
  WiFi.softAP("FP-Config", "attendance");
  Serial.printf("[AP] SSID=FP-Config pass=attendance IP=%s\n", WiFi.softAPIP().toString().c_str());
  http.on("/", HTTP_GET, []() { http.send_P(200, "text/html", PORTAL_HTML); });
  http.onNotFound([]() { http.sendHeader("Location", "/"); http.send(302, "text/plain", ""); });
  http.begin();
  wsServer.begin();
  wsServer.onEvent(wsEvent);
  xTaskCreatePinnedToCore(ConfigTask, "Config", 10240, NULL, 3, &hConfig, 1);
}

// ============================== NORMAL MODE ==============================
static void startNormalMode() {
  Serial.println("[MODE] NORMAL MODE");
  if (!EEPROM.begin(EEPROM_SIZE)) Serial.println("[EEPROM] begin failed!");
  loadStudentsFromEEPROM();
  loadOfflineAttendanceFromEEPROM();

  qDisplay = xQueueCreate(16, sizeof(DisplayMsg));
  qLed     = xQueueCreate(8,  sizeof(LedMsg));
  qComm    = xQueueCreate(12, sizeof(CommItem));
  qEspRx   = xQueueCreate(8,  sizeof(FpMsg));
  qMqttOut = xQueueCreate(12, sizeof(MqttOut));
  qAtt     = xQueueCreate(8,  sizeof(Attendance));
  qStore   = xQueueCreate(8,  sizeof(StoreOp));
  qFpCmd   = xQueueCreate(6,  sizeof(FpCmd));
  mtxData  = xSemaphoreCreateMutex();
  semAck   = xSemaphoreCreateBinary();
  evGroup  = xEventGroupCreate();

  esp_reset_reason_t rr = esp_reset_reason();
  if (rr == ESP_RST_TASK_WDT || rr == ESP_RST_INT_WDT || rr == ESP_RST_WDT || rr == ESP_RST_PANIC)
    bootMsg("Recovered from\nWatchdog/panic reset!", 2500);

  bootMsg("System Booting...");
  mySerial.begin(57600, SERIAL_8N1, RX_PIN, TX_PIN);
  delay(100);
  bootMsg("Checking AS608...");
  while (!finger.verifyPassword()) {
    bootMsg("AS608 NOT FOUND!\nCheck wiring");
    digitalWrite(RED_LED, HIGH);
    delay(1500);
  }
  digitalWrite(RED_LED, LOW);
  bootMsg("AS608 Found!");

  sntp_set_time_sync_notification_cb(ntpSyncCallback);
  initEspNow();

  // ---- tasks (A1): priority 4 = most urgent. Net/Comm last: they use hFinger ----
  xTaskCreatePinnedToCore(FingerprintTask, "Finger", 6144, NULL, 3, &hFinger,    1);
  xTaskCreatePinnedToCore(DisplayTask,     "Disp",   5120, NULL, 2, &hDisplay,   1);
  xTaskCreatePinnedToCore(StorageTask,     "Store",  4096, NULL, 2, &hStorage,   1);
  xTaskCreatePinnedToCore(HeartbeatTask,   "Heart",  4096, NULL, 2, &hHeartbeat, 1);
  xTaskCreatePinnedToCore(ButtonTask,      "Button", 3072, NULL, 2, &hButton,    1);
  xTaskCreatePinnedToCore(LedTask,         "Led",    2048, NULL, 1, &hLed,       1);
  xTaskCreatePinnedToCore(CommTask,        "Comm",   4096, NULL, 4, &hComm,      0);
  xTaskCreatePinnedToCore(NetTask,         "Net",   12288, NULL, 3, &hNet,       0);

  // ---- interrupts (A3): attached only after the tasks they notify exist ----
  pinMode(BTN_PIN, INPUT_PULLUP);
  attachInterrupt(digitalPinToInterrupt(BTN_PIN), isrButton, FALLING);
  if (cfg.useWakePin) {
    pinMode(WAKE_PIN, INPUT);
    attachInterrupt(digitalPinToInterrupt(WAKE_PIN), isrWake, CHANGE);
    Serial.println("[ISR] AS608 WAKEUP interrupt enabled");
  }
  hbTimer = timerBegin(1000000);                       // 1 MHz tick
  timerAttachInterrupt(hbTimer, &isrTimer1Hz);
  timerAlarm(hbTimer, 1000000, true, 0);               // 1 Hz, auto-reload
}

void setup() {
  Serial.begin(115200);
  delay(300);
  Serial.printf("\n--- Fingerprint Station (FreeRTOS) --- reset reason: %s\n", resetReasonStr());

  pinMode(GREEN_LED, OUTPUT);
  pinMode(RED_LED, OUTPUT);
  digitalWrite(GREEN_LED, LOW);
  digitalWrite(RED_LED, LOW);
  pinMode(BTN_PIN, INPUT_PULLUP);

  loadConfig();
  mtxI2C = xSemaphoreCreateMutex();

  Wire.begin(OLED_SDA, OLED_SCL);
  Wire.setClock(400000);
  if (!display.begin(0x3C, true)) {
    Serial.println("[OLED] init failed");
    while (1) delay(10);
  }
  display.setRotation(0);
  initWatchdog();
  delay(100);

  bool configMode = (digitalRead(BTN_PIN) == LOW) || !cfgValid();   // button held during reset
  if (configMode) startConfigMode();
  else            startNormalMode();
}

void loop() {
  vTaskDelete(NULL);          // everything runs in FreeRTOS tasks
}