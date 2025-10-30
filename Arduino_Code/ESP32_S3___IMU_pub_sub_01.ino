#include <WiFi.h>
#include <Wire.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>

/* ========= USER CONFIG ========= */
const char* WIFI_SSID     = "WIFI_SSID";
const char* WIFI_PASSWORD = "WIFI_PASS";
const char* MQTT_BROKER   = "192.168.1.1";
const uint16_t MQTT_PORT  = 1883;

/* Topics */
const char* TOP_HEARTBEAT = "esp32/test/heartbeat";
const char* TOP_STATUS    = "esp32/test/status";
const char* TOP_CMD       = "rpi/cmd";
const char* TOP_IMU       = "smarthome/imu";   // <-- NEW: IMU JSON stream

/* Optional on-board LED (set to -1 if unused) */
#define LED_PIN -1
/* ================================= */

/* ------------ MQTT ------------ */
WiFiClient espClient;
PubSubClient mqtt(espClient);
char clientId[40];

/* ------------ Timers ------------ */
unsigned long lastHeartbeatMs = 0;
unsigned long lastImuPublishMs = 0;
const uint16_t HEARTBEAT_EVERY_MS = 5000; // 5 s
const uint16_t PUBLISH_EVERY_MS   = 50;   // 50 ms ≈ 20 Hz publish (change to 10 for ~100 Hz)

/* ------------ IMU (MPU-6500) ------------ */
/* Registers */
#define REG_PWR_MGMT_1     0x6B
#define REG_PWR_MGMT_2     0x6C
#define REG_SMPLRT_DIV     0x19
#define REG_CONFIG         0x1A
#define REG_ACCEL_CONFIG   0x1C
#define REG_ACCEL_XOUT_H   0x3B

/* State */
uint8_t  mpuAddr = 0;                 // 0x68 or 0x69 (auto-detected)
const float LSB_PER_G = 8192.0f;      // ±4 g scale

/* Sample timing for 100 Hz sensor read */
const float   FS_HZ = 100.0f;
const uint16_t DT_MS = 10;
uint32_t nextTick = 0;

/* High-pass filter on magnitude (remove gravity/tilt) */
const float HP_FC_HZ = 0.5f;
float hp_alpha = 0.0f, mag_prev = 0.0f, hp_prev = 0.0f;

/* Sliding RMS (ring buffer) over ~0.25 s */
const uint16_t RMS_WIN = 25;
float    rmsBuf[RMS_WIN];
uint16_t rmsCount = 0;
uint16_t rmsHead  = 0;
float    rmsSumSq = 0.0f;

/* Classification thresholds (g RMS) */
float TH_STRUCT = 0.03f, TH_FOOT = 0.10f, TH_KID = 0.20f, TH_JUMP = 0.35f;

/* ---------- Utils ---------- */
static float hpAlpha(const float fs, const float fc) {
  const float dt = 1.0f / fs;
  const float RC = 1.0f / (2.0f * 3.1415926f * fc);
  return RC / (RC + dt);
}

static void i2cWrite8(uint8_t addr, uint8_t reg, uint8_t val) {
  Wire.beginTransmission(addr);
  Wire.write(reg);
  Wire.write(val);
  Wire.endTransmission();
}

static bool i2cReadN(uint8_t addr, uint8_t reg, uint8_t* buf, uint8_t n) {
  Wire.beginTransmission(addr);
  Wire.write(reg);
  if (Wire.endTransmission(false) != 0) return false;
  if (Wire.requestFrom(addr, n) != n) return false;
  for (uint8_t i = 0; i < n; ++i) buf[i] = Wire.read();
  return true;
}

static bool mpuInit(uint8_t addrProbeA = 0x68, uint8_t addrProbeB = 0x69) {
  // scan bus to pick 0x68/0x69
  for (uint8_t a = 1; a < 127; ++a) {
    Wire.beginTransmission(a);
    if (Wire.endTransmission() == 0) {
      if ((a == addrProbeA || a == addrProbeB) && mpuAddr == 0) mpuAddr = a;
    }
  }
  // fallback probe
  if (mpuAddr == 0) {
    Wire.beginTransmission(addrProbeA);
    if (Wire.endTransmission() == 0) mpuAddr = addrProbeA;
    else {
      Wire.beginTransmission(addrProbeB);
      if (Wire.endTransmission() == 0) mpuAddr = addrProbeB;
    }
  }
  if (mpuAddr == 0) return false;

  // Reset + wake + config
  i2cWrite8(mpuAddr, REG_PWR_MGMT_1, 0x80); delay(100);
  i2cWrite8(mpuAddr, REG_PWR_MGMT_1, 0x01); delay(10);
  i2cWrite8(mpuAddr, REG_PWR_MGMT_2, 0x00);
  i2cWrite8(mpuAddr, REG_CONFIG,       0x03); // DLPF ~44 Hz
  i2cWrite8(mpuAddr, REG_ACCEL_CONFIG, 0x08); // ±4 g
  i2cWrite8(mpuAddr, REG_SMPLRT_DIV,   0x00);
  delay(10);

  hp_alpha = hpAlpha(FS_HZ, HP_FC_HZ);
  nextTick = millis();
  return true;
}

static bool readAccel_g(float& ax, float& ay, float& az) {
  uint8_t raw[6];
  if (!i2cReadN(mpuAddr, REG_ACCEL_XOUT_H, raw, 6)) return false;
  const int16_t ax_raw = (int16_t)((raw[0] << 8) | raw[1]);
  const int16_t ay_raw = (int16_t)((raw[2] << 8) | raw[3]);
  const int16_t az_raw = (int16_t)((raw[4] << 8) | raw[5]);
  ax = ax_raw / LSB_PER_G;
  ay = ay_raw / LSB_PER_G;
  az = az_raw / LSB_PER_G;
  return true;
}

static float rmsPush(float sampleAbs) {
  const float s2 = sampleAbs * sampleAbs;
  if (rmsCount < RMS_WIN) {
    rmsBuf[rmsHead] = sampleAbs;
    rmsSumSq += s2;
    rmsHead = (rmsHead + 1) % RMS_WIN;
    rmsCount++;
  } else {
    const uint16_t tail = rmsHead;
    const float old = rmsBuf[tail];
    rmsSumSq -= old * old;
    rmsBuf[tail] = sampleAbs;
    rmsSumSq += s2;
    rmsHead = (rmsHead + 1) % RMS_WIN;
  }
  const uint16_t n = rmsCount ? rmsCount : 1;
  return sqrtf(rmsSumSq / n);
}

static const char* classifyLabel(float rms) {
  if      (rms >= TH_JUMP)   return "JUMP";
  else if (rms >= TH_KID)    return "PLAY";
  else if (rms >= TH_FOOT)   return "FOOT";
  else if (rms >= TH_STRUCT) return "STRUCT";
  return "CALM";
}

/* ------------ MQTT callback (optional LED + status) ------------ */
void mqttCallback(char* topic, byte* payload, unsigned int length) {
  String msg; msg.reserve(length);
  for (unsigned int i = 0; i < length; i++) msg += (char)payload[i];
  Serial.printf("[MQTT RX] %s -> %s\n", topic, msg.c_str());

  String cmd;
  StaticJsonDocument<128> doc;
  if (deserializeJson(doc, msg) == DeserializationError::Ok && doc.containsKey("cmd"))
    cmd = doc["cmd"].as<String>();
  else
    cmd = msg;
  cmd.toLowerCase();

  if (cmd == "ping") {
    mqtt.publish(TOP_STATUS, "pong", true);
  } else if (cmd == "led:on") {
#if LED_PIN >= 0
    digitalWrite(LED_PIN, HIGH);
    mqtt.publish(TOP_STATUS, "LED ON", true);
#else
    mqtt.publish(TOP_STATUS, "LED unavailable", true);
#endif
  } else if (cmd == "led:off") {
#if LED_PIN >= 0
    digitalWrite(LED_PIN, LOW);
    mqtt.publish(TOP_STATUS, "LED OFF", true);
#else
    mqtt.publish(TOP_STATUS, "LED unavailable", true);
#endif
  } else if (cmd == "led:toggle") {
#if LED_PIN >= 0
    digitalWrite(LED_PIN, !digitalRead(LED_PIN));
    mqtt.publish(TOP_STATUS, "LED toggled", true);
#else
    mqtt.publish(TOP_STATUS, "LED unavailable", true);
#endif
  } else {
    mqtt.publish(TOP_STATUS, "Unknown cmd", false);
  }
}

/* ------------ Connectivity helpers ------------ */
void ensureWifi() {
  if (WiFi.status() == WL_CONNECTED) return;
  Serial.printf("Connecting to WiFi: %s\n", WIFI_SSID);
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  while (WiFi.status() != WL_CONNECTED) {
    delay(300);
    Serial.print(".");
  }
  Serial.printf("\nWiFi OK. IP: %s RSSI: %d dBm\n", WiFi.localIP().toString().c_str(), WiFi.RSSI());
}

void ensureMqtt() {
  if (mqtt.connected()) return;
  mqtt.setServer(MQTT_BROKER, MQTT_PORT);
  mqtt.setCallback(mqttCallback);
  mqtt.setKeepAlive(30);
  mqtt.setSocketTimeout(3);
  mqtt.setBufferSize(512); // allow JSON IMU payload comfortably

  snprintf(clientId, sizeof(clientId), "esp32s3-%08lx", (uint32_t)ESP.getEfuseMac());

  while (!mqtt.connected()) {
    Serial.print("Connecting MQTT...");
    if (mqtt.connect(clientId, nullptr, nullptr,
                     TOP_STATUS, 1, true, "offline")) {
      Serial.println("OK");
      mqtt.publish(TOP_STATUS, "online", true);
      mqtt.subscribe(TOP_CMD, 1);
    } else {
      Serial.printf("failed rc=%d, retry in 2s\n", mqtt.state());
      delay(2000);
    }
  }
}

/* ===================== SETUP ===================== */
void setup() {
  Serial.begin(115200);
  delay(100);

#if LED_PIN >= 0
  pinMode(LED_PIN, OUTPUT);
  digitalWrite(LED_PIN, LOW);
#endif

  /* ESP32-S3 default I²C pins are used by calling Wire.begin() with no args.
     (On ESP32-S3 DevKitC-1 variants, this maps to SDA=GPIO8, SCL=GPIO9 by default.) */
  Wire.begin();
  Wire.setClock(400000);

  ensureWifi();
  ensureMqtt();

  if (!mpuInit()) {
    Serial.println("# ERROR: MPU-6500 not found at 0x68/0x69");
    mqtt.publish(TOP_STATUS, "imu_not_found", true);
    // keep running for WiFi/MQTT; IMU publishes will be skipped
  } else {
    Serial.printf("# Using MPU-6500 at 0x%02X\n", mpuAddr);
    mqtt.publish(TOP_STATUS, "imu_online", true);
  }

  // CSV-style header (debug)
  Serial.println("#HDR ms,ax,ay,az,mag,hp_abs,rms,label");
}

/* ===================== LOOP ===================== */
void loop() {
  ensureWifi();
  ensureMqtt();
  mqtt.loop();

  const unsigned long now = millis();

  /* ---- 100 Hz IMU sampling ---- */
  if ((int32_t)(now - nextTick) >= 0) {
    nextTick += DT_MS;

    if (mpuAddr != 0) {
      float ax=0, ay=0, az=0;
      if (readAccel_g(ax, ay, az)) {
        const float mag = sqrtf(ax*ax + ay*ay + az*az);
        const float hp  = hp_alpha * (hp_prev + (mag - mag_prev));
        hp_prev = hp; mag_prev = mag;
        const float hp_abs = fabsf(hp);
        const float rms = rmsPush(hp_abs);
        const char* label = classifyLabel(rms);

        // Debug print (optional)
        // Serial.printf("%lu,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%s\n",
        //               now, ax, ay, az, mag, hp_abs, rms, label);

        // Publish at chosen rate (20 Hz by default)
        if (now - lastImuPublishMs >= PUBLISH_EVERY_MS) {
          lastImuPublishMs = now;

          StaticJsonDocument<256> j;
          j["ms"]     = now;
          j["ax"]     = ax;
          j["ay"]     = ay;
          j["az"]     = az;
          j["mag"]    = mag;
          j["hp_abs"] = hp_abs;
          j["rms"]    = rms;
          j["label"]  = label;

          char buf[256];
          size_t n = serializeJson(j, buf, sizeof(buf));
          mqtt.publish(TOP_IMU, (const uint8_t*)buf, n, /*retain*/ false); // QoS0 in PubSubClient
        }
      }
    }
  }

  /* ---- Heartbeat every 5 s ---- */
  if (now - lastHeartbeatMs >= HEARTBEAT_EVERY_MS) {
    lastHeartbeatMs = now;
    StaticJsonDocument<192> hb;
    hb["device"]    = "esp32-s3-devkitc-1";
    hb["uptime_ms"] = now;
    hb["ip"]        = WiFi.localIP().toString();
    hb["rssi_dbm"]  = WiFi.RSSI();
    char buf[192];
    size_t n = serializeJson(hb, buf, sizeof(buf));
    mqtt.publish(TOP_HEARTBEAT, (const uint8_t*)buf, n, false);
  }

  delay(2); // be kind to CPU
}
