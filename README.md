# GPS RTK & PPK System Node

A robust Rust application designed to interface with **U-blox** and **Unicore** GNSS receivers over USB-UART. The system includes an **NTRIP Client** for RTK corrections, raw logging for **PPK**, **1Hz MQTT telemetry** reporting, and a **Supervisor Watchdog** to monitor and automatically restart crashed or frozen tasks.

---

## Features

- **Multi-protocol Parsing:** Stream-based parser that handles NMEA (GGA, RMC, HDT), U-blox binary (`UBX-NAV-PVT`), and Unicore ASCII (`#BESTPOSA`, `#HEADINGA`).
- **NTRIP Client:** Fetches sourcetables (mountpoint list) and streams RTCM corrections. Supports Virtual Reference Station (VRS) by dynamically uploading GGA sentences.
- **PPK Raw Logger & Auto-Configuration:** Automatically configures the GNSS receiver at boot (enabling UBX-RXM-RAWX/SFRBX for U-blox or rangesb/ephemerisb for Unicore) to output raw measurements, and logs these raw bytes directly into hourly rotating binary files.
- **MQTT Reporting:** Formats and publishes coordinate and fix status JSON telemetry at 1Hz.
- **Independent Component Watchdog:** Monitors tasks using a centralized message router, allowing individual component restarts (e.g. restarting NTRIP while keeping Serial/PPK active).

---

## Configuration (`config.json`)

The node is completely configured via `config.json` located in the root of the project:

```json
{
  "general": {
    "device_type": "ublox",          // Receiver type: "ublox" or "unicore"
    "log_directory": "./logs",       // Directory for raw PPK binary logs
    "log_rotation_hours": 1          // Rotate PPK logs hourly
  },
  "serial": {
    "port": "/dev/ttyUSB0",          // Serial port path
    "baud_rate": "auto"              // Set to numeric (e.g. 115200) or "auto" to scan and detect
  },
  "ntrip": {
    "enabled": true,
    "caster_host": "rtk2go.com",     // NTRIP caster host
    "caster_port": 2101,             // NTRIP caster port
    "username": "your_username",     // Caster login username
    "password": "your_password",     // Caster login password
    "mountpoint": "TEST_RTCM3"       // Leave empty ("") to query the mountpoint list and exit
  },
  "mqtt": {
    "enabled": true,
    "broker_host": "broker.hivemq.com",
    "broker_port": 1883,
    "client_id": "gps_rtk_node_01",
    "topic": "gps/telemetry",        // Topic to publish 1Hz JSON telemetry
    "username": "",
    "password": ""
  },
  "watchdog": {
    "check_interval_secs": 2,        // Frequency of heartbeat verification
    "heartbeat_timeout_secs": 10     // Seconds without heartbeat before triggering restart
  }
}
```

### Configuring the Serial Port

To find your GPS USB-UART serial port path:

- **macOS:**
  Open a terminal and run:
  ```bash
  ls /dev/tty.usbserial-*
  # or
  ls /dev/tty.usbmodem*
  ```
  Copy the path (e.g., `/dev/tty.usbserial-10`) and paste it as the `"port"` value in `config.json`.

- **Linux:**
  Open a terminal and run:
  ```bash
  ls /dev/ttyUSB*
  # or
  ls /dev/ttyACM*
  ```
  Copy the path (e.g., `/dev/ttyUSB0`) and paste it as the `"port"` value in `config.json`.

---

## How to Run

### Prerequisite

Make sure you have Rust and Cargo installed. (Minimum version 1.81+ required).

### Step 1: Run Unit Tests

Verify that the parsing, Fletcher checksumming, and CRC32 verification are operating correctly:

```bash
cargo test
```

### Step 2: Build and Run

To compile and launch the node in release mode (highly recommended for deployment):

```bash
cargo run --release
```

To run in debug mode with full console log output:

```bash
RUST_LOG=debug cargo run
```

---

## Component-Level Recovery & Restart Behavior

If a serial connection is lost (e.g., cable disconnected) or the NTRIP connection drops, the supervisor watchdog will:
1. Detect that the specific task's thread/handle has terminated or is frozen.
2. Log the critical error in the console identifying the crashed component.
3. Keep the other components running (e.g. keeping serial PPK logger active if NTRIP is reconnecting).
4. Wait 5 seconds.
5. Re-initialize and restart **only** the failed component task, automatically resolving dependencies and reconnecting.
