# ESP32 IoT Student Fingerprint Attendance System

An attendance system with **two ESP32 boards**, an AS608 fingerprint sensor, a Node.js MQTT-to-Firebase bridge and a React dashboard.

**Version 2.0 (CSE4052 Part 2)** adds FreeRTOS, interrupts, a WebSocket config portal, two boards over ESP-NOW, NVS storage and a task watchdog.

## Overview

| Part | Job |
|------|-----|
| **Station (ESP32)** | Fingerprint scan/enrol, OLED, Wi-Fi, NTP, MQTT, offline storage, config portal |
| **Aux node (ESP32-C3)** | LED feedback, remote pause/resume button, station health alarm |
| **Bridge (Node.js)** | MQTT to Firebase, timestamp check, duplicate removal |
| **Dashboard (React)** | Students, attendance, time table, modules, CSV reports |

```
ESP32 Station ◄── ESP-NOW ──► ESP32-C3 Aux node
     │ Wi-Fi + MQTT (TLS 8883)
     ▼
HiveMQ Cloud ─► Node.js bridge ─► Firebase RTDB ─► React dashboard
```

## Part 2 Features

| Req. | Feature | Code |
|------|---------|------|
| A1 | FreeRTOS multitasking (9 tasks on Station, 4 on Aux) | `startNormalMode()`, `*Task()` |
| A2 | WebSocket config portal (SoftAP + HTML) | `PORTAL_HTML`, `startConfigMode()`, `wsEvent()` |
| A3 | Interrupts: 1 Hz timer, AS608 WAKEUP pin, buttons | `isrTimer1Hz`, `isrWake`, `isrButton` |
| A4 | Two ESP32 boards over ESP-NOW (ACK + retry) | `initEspNow()`, `sendReliable()`, `CommTask` |
| B1 | NVS settings storage | `loadConfig()`, `saveConfig()` |
| B2 | Task watchdog (10 s Station, 8 s Aux) | `initWatchdog()` |
| B4 | Queues, mutexes, semaphore, event group, notifications | `startNormalMode()` |


## Tech Stack

- **Firmware:** Arduino IDE, esp32 board package 3.x, FreeRTOS, ESP-NOW, NVS, EEPROM. Libraries: Adafruit SH110X, Adafruit GFX, Adafruit Fingerprint, PubSubClient, ArduinoJson v6, WebSockets (Links2004)
- **Backend:** Node.js, mqtt, firebase-admin, dotenv
- **Frontend:** React, Vite, TailwindCSS, Recharts, Firebase

## Hardware

**Station (ESP32 DevKit)**

| Part | Pin |
|------|-----|
| AS608 TX / RX | GPIO18 / GPIO19 |
| AS608 WAKEUP (optional, 6-wire module) | GPIO27 |
| OLED SH1107 SDA / SCL (I2C `0x3C`) | GPIO21 / GPIO22 |
| Green / Red LED (330 Ω) | GPIO2 / GPIO4 |
| Push button to GND | GPIO13 |

**Aux node (ESP32-C3)**

| Part | Pin |
|------|-----|
| Green / Red LED (330 Ω) | GPIO5 / GPIO6 |
| Push button to GND | GPIO7 |

Do not use GPIO0 for the button (download-mode pin). AS608 baud rate is 57600.


## Config Portal

1. **Hold the button (GPIO13) and press reset.** It also opens on first use when no settings are saved.
2. Join Wi-Fi **`FP-Config`**, password **`attendance`**.
3. Open **http://192.168.4.1**.
4. Enter Wi-Fi, MQTT, Aux MAC and timing settings, then press **Save** or **Save and reboot**.

Settings are stored in NVS and survive power loss. Passwords are never sent back to the browser; an empty password field keeps the stored one. After **Save and reboot** the setup network closes. Hold the button during reset to open it again.

## Usage

- **Match:** OLED shows the name, green LED, record goes to Firebase, Aux blinks green.
- **No match:** red LED, Aux blinks red.
- **Offline:** record is saved in EEPROM (30 records) and synced when Wi-Fi, MQTT and time return.
- **Enrol:** use the dashboard **Register student** form, then scan the finger twice.

**Station button (GPIO13):** short press shows diagnostics; hold 3 s runs the watchdog demo.

**Aux button (GPIO7):** short press pauses/resumes scanning; hold 2.5 s sends the watchdog test.

| Aux LED | Meaning |
|---------|---------|
| Green 3 blinks | Match / enrol OK |
| Red 2 long blinks | No match / enrol failed |
| Green + red together | Saved offline |
| Both 2 quick blinks | Station link restored |
| Red fast blink | **Station offline** (no heartbeat for 5 s) |
| Green solid / slow blink | Cloud OK / cloud down |
| Red solid | Station paused |

## How It Works

**Tasks (Station):** Comm (prio 4, core 0), Net (3, core 0), Fingerprint (3), Display (2), Storage (2), Heartbeat (2), Button (2), Led (1), Config (3, config mode only). **Aux:** Rx (4), Monitor (3), Led (2), Button (2).

**Interrupts:** ISRs only call `vTaskNotifyGiveFromISR()`; all work runs in tasks. Buttons are debounced (200 ms in the ISR, 30 ms in the task).

**Inter-task communication:** queues, mutexes (`mtxI2C`, `mtxData`), binary semaphore (ESP-NOW ACK), event group (Wi-Fi, MQTT, NTP, peer link), task notifications.

**Watchdog:** every task feeds the task watchdog. After a watchdog reset the OLED shows "Recovered from Watchdog/panic reset!".

**Storage:** NVS `fpcfg` (Station settings), NVS `fpnode` (Aux ESP-NOW channel), EEPROM (students, max 50, and offline records, same layout as Part 1).

## ESP-NOW Protocol

Both boards use one packed 44-byte packet (`FpMsg`, magic `0xA5`).

| Type | Direction | Meaning |
|------|-----------|---------|
| HEARTBEAT | Station → Aux, every 1 s | Link check + status flags |
| ATT_EVENT | Station → Aux | Match / no match / offline / enrol result |
| CMD | Aux → Station | Pause, resume, watchdog test |
| ACK | Both ways | Confirms a sequence number |

Events and commands are retried up to 3 times. ESP-NOW needs both boards on the same channel, so the Aux node searches channels 1–13, saves the working channel in NVS and learns the Station MAC from the first heartbeat.

## MQTT Topics

| Topic | Direction | Purpose |
|-------|-----------|---------|
| `fp/attendance` | Station → bridge | Attendance record |
| `fp/enrolled` | Station → bridge | New student saved |
| `fp/heartbeat` | Station → bridge | Station alive |
| `fp/message` | Station → bridge | Live enrol messages |
| `fp/stateAck` | Station → bridge | State confirmed |
| `fp/systemState` | Bridge → Station | `ENROLL` or `VERIFY` |
| `fp/enrollData` | Bridge → Station | `{name, regNum}` for enrolment |

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `Node2:X` on the OLED | Check the Aux MAC in the portal, power on the Aux node, wait up to 20 s |
| Aux red LED blinks fast | No heartbeat from the Station (off, crashed or on another channel) |
| `AS608 NOT FOUND` | Check TX/RX are crossed and the power |
| WAKEUP does nothing | Needs a 6-wire AS608 and the portal option **WAKEUP pin interrupt** |
| `No time sync!` | Check internet and NTP; records are stored offline until synced |
| MQTT keeps failing | Check host, port 8883, username and password (portal and `.env`) |
| Compile error on `esp_task_wdt` / `timerBegin` | Use esp32 board package 3.x |
| Firebase error | Check `serviceAccountKey.json` and `FIREBASE_DB_URL` |

## License

MIT

**Author:** Arulaiah Kajanthan · [GitHub](https://github.com/kajanthann/IOT_Group_Project_Part_02)