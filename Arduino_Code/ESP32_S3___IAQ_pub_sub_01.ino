/*
ESP32-S3 IAQ Publisher (SCD41 + BME680) → MQTT (Raspberry Pi)
- Publishes JSON every 2s with fields matching your CSV header
- Heartbeat + availability (LWT)
- Command topic support (simple "ping" -> "pong")

Hardware:
- ESP32-S3 (e.g., SuperMini). Adjust I2C pins if needed.
- SCD41 at 0x62
- BME680 at 0x76 or 0x77

Libraries (Arduino Library Manager):
- PubSubClient by Nick O'Leary
- Sensirion I2C SCD4x (SensirionI2cScd4x)
- Adafruit BME680 Library (+ Adafruit BusIO & Adafruit Unified Sensor)
*/

#include <WiFi.h>
#include <PubSubClient.h>
#include <Wire.h>
#include <SensirionI2cScd4x.h>
#include <Adafruit_BME680.h>

/* ---------- USER CONFIG ---------- */
#define WIFI_SSID    "WIFI_SSID"
#define WIFI_PASS    "WIFI_PASS"

// Your Raspberry Pi broker:
#define MQTT_HOST    "192.168.1.1"
#define MQTT_PORT    1883

// Topic layout (adjust as you like)
#define TOP_BASE       "esp32/iaq"
#define TOP_TELEMETRY  TOP_BASE "/telemetry"   // JSON payload every 2s
#define TOP_HEARTBEAT  TOP_BASE "/heartbeat"   // short "beat" text
#define TOP_STATUS     TOP_BASE "/status"      // "online"/"offline" retained
#define TOP_CMD        "rpi/cmd"               // simple command channel

// MQTT client id
String CLIENT_ID = String("esp32s3-iaq-") + String((uint32_t)ESP.getEfuseMac(), HEX);

// I2C pins — adjust to your board
#define I2C_SDA  8
#define I2C_SCL  9

/* ---------- STATUS THRESHOLDS (match your Arduino Mega UI) ---------- */
static const uint16_t CO2_NORMAL_MAX   = 800;   // <800 -> Normal
static const uint16_t CO2_ELEVATED_MAX = 1200;  // 800..1199 -> Elevated; >=1200 -> High

/* ---------- GAS INDEX (same EMA from your code) ---------- */
float gasBaseline = NAN;
const float GAS_EMA_ALPHA = 0.01f;

/* ---------- GLOBALS ---------- */
WiFiClient wifiClient;
PubSubClient mqtt(wifiClient);

SensirionI2cScd4x scd4x;
Adafruit_BME680 bme;

bool bmeOK = false;
uint8_t scd_addr = 0x62;

unsigned long lastPubMs = 0;
const unsigned long PUB_INTERVAL_MS = 2000;

// Forward decls
void connectWiFi();
void connectMQTT();
void onMqttMessage(char* topic, byte* payload, unsigned int length);
float computeGasIndex(float gasOhms);
const char* iaqWordFromGas(float gasOhm);
const char* co2WordFromPpm(uint16_t ppm);

/* -------------------- GAS INDEX -------------------- */
float computeGasIndex(float gasOhms){
  if (isnan(gasBaseline)) gasBaseline = gasOhms;
  gasBaseline = GAS_EMA_ALPHA * gasOhms + (1.0f - GAS_EMA_ALPHA) * gasBaseline;
  float ratio = gasBaseline / (gasOhms <= 0 ? 1.0f : gasOhms);
  float idx   = 50.0f + 50.0f * (ratio - 1.0f);
  if (idx < 0)   idx = 0;
  if (idx > 100) idx = 100;
  return idx;
}

const char* iaqWordFromGas(float gasOhm){
  // Good ≥ 80 kΩ; Moderate 40–79 kΩ; Poor < 40 kΩ
  if (gasOhm >= 80000.0f) return "Good";
  if (gasOhm >= 40000.0f) return "Moderate";
  return "Poor";
}
const char* co2WordFromPpm(uint16_t ppm){
  if (ppm < CO2_NORMAL_MAX)   return "Normal";
  if (ppm < CO2_ELEVATED_MAX) return "Elevated";
  return "High";
}

/* -------------------- WIFI -------------------- */
void connectWiFi(){
  if (WiFi.status() == WL_CONNECTED) return;
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASS);
  Serial.printf("WiFi: connecting to %s ...\n", WIFI_SSID);

  uint8_t tries = 0;
  while (WiFi.status() != WL_CONNECTED && tries < 60){
    delay(250);
    Serial.print('.');
    tries++;
  }
  Serial.println();
  if (WiFi.status() == WL_CONNECTED){
    Serial.print("WiFi: connected, IP=");
    Serial.println(WiFi.localIP());
  } else {
    Serial.println("WiFi: failed to connect");
  }
}

/* -------------------- MQTT -------------------- */
void onMqttMessage(char* topic, byte* payload, unsigned int length){
  String t = topic;
  String msg;
  msg.reserve(length+1);
  for (unsigned int i=0;i<length;i++) msg += (char)payload[i];

  Serial.printf("[MQTT RX] %s -> %s\n", t.c_str(), msg.c_str());

  // trivial command handler
  if (t == TOP_CMD){
    if (msg.equalsIgnoreCase("ping")){
      mqtt.publish(TOP_HEARTBEAT, "pong", false);
    } else if (msg.equalsIgnoreCase("pub")){
      // force an immediate publish on demand
      lastPubMs = 0;
    }
  }
}

void connectMQTT(){
  if (mqtt.connected()) return;

  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  mqtt.setCallback(onMqttMessage);

  // LWT: if we drop, broker sets retained "offline"
  const char* willTopic = TOP_STATUS;
  const char* willMsg   = "offline";
  bool willRetain = true;
  uint8_t willQos = 1;

  Serial.printf("MQTT: connecting to %s:%d as %s ...\n",
                MQTT_HOST, MQTT_PORT, CLIENT_ID.c_str());

  uint8_t attempts = 0;
  while (!mqtt.connected() && attempts < 10){
    if (mqtt.connect(CLIENT_ID.c_str(), NULL, NULL,
                     willTopic, willQos, willRetain, willMsg)){
      Serial.println("MQTT: connected");

      // Mark online (retained)
      mqtt.publish(TOP_STATUS, "online", true);

      // subscriptions
      mqtt.subscribe(TOP_CMD, 1);

      // heartbeat (non-retained)
      mqtt.publish(TOP_HEARTBEAT, "boot", false);
      break;
    } else {
      Serial.printf("MQTT: failed rc=%d, retrying...\n", mqtt.state());
      delay(500);
      attempts++;
    }
  }
}

/* -------------------- SETUP -------------------- */
void setup() {
  Serial.begin(115200);
  delay(200);

  Wire.begin(I2C_SDA, I2C_SCL); // set pins explicitly for S3

  connectWiFi();
  connectMQTT();

  // ---- SCD41 ----
  scd4x.begin(Wire, 0x62);
  scd4x.stopPeriodicMeasurement();  // ensure clean start
  uint16_t err = scd4x.startPeriodicMeasurement();
  if (err) {
    Serial.print("SCD41 start error: "); Serial.println(err);
  } else {
    Serial.println("SCD41: periodic measurement started");
  }

  // ---- BME680 ----
  if (bme.begin(0x76) || bme.begin(0x77)) {
    bmeOK = true;
    bme.setTemperatureOversampling(BME680_OS_8X);
    bme.setHumidityOversampling(BME680_OS_2X);
    bme.setPressureOversampling(BME680_OS_4X);
    bme.setIIRFilterSize(BME680_FILTER_SIZE_3);
    bme.setGasHeater(320, 150);
    Serial.println("BME680: ready");
  } else {
    Serial.println("BME680: NOT found (0x76/0x77)");
  }

  // Helpful header for any serial logging you still want
  delay(1000);
  Serial.println(F("#HDR ms,co2_ppm,temp_scd,hum_scd,temp_bme,hum_bme,press_hpa,gas_ohm,iaq_index,iaq_status,co2_status"));
}

/* -------------------- LOOP -------------------- */
void loop() {
  // Keep connections healthy
  if (WiFi.status() != WL_CONNECTED) connectWiFi();
  if (!mqtt.connected()) connectMQTT();
  mqtt.loop();

  static uint16_t lastCO2 = 0;
  static float lastT_scd = NAN, lastRH_scd = NAN;

  // --- SCD41: data ready roughly every ~5s ---
  bool ready = false;
  uint16_t err = scd4x.getDataReadyStatus(ready);
  if (!err && ready) {
    uint16_t co2 = 0; float tC = NAN, rh = NAN;
    if (!scd4x.readMeasurement(co2, tC, rh) && co2 != 0) {
      lastCO2 = co2; lastT_scd = tC; lastRH_scd = rh;
    }
  }

  // --- BME680 ---
  float t_bme=NAN, rh_bme=NAN, hPa=NAN, gas= NAN, gIdx=NAN;
  if (bmeOK && bme.performReading()){
    t_bme  = bme.temperature;
    rh_bme = bme.humidity;
    hPa    = bme.pressure / 100.0f;
    gas    = bme.gas_resistance;
    gIdx   = computeGasIndex(gas);
  }

  // --- Publish every 2 seconds ---
  unsigned long now = millis();
  if (now - lastPubMs >= PUB_INTERVAL_MS){
    lastPubMs = now;

    const char* iaqStatus = iaqWordFromGas(isnan(gas)?0:gas);
    const char* co2Status = co2WordFromPpm(lastCO2);

    // JSON payload (matches your CSV header names)
    // (Keep numbers compact; NaNs become 0 for simplicity.)
    char payload[512];
    snprintf(payload, sizeof(payload),
      "{\"ms\":%lu,"
      "\"co2_ppm\":%u,"
      "\"temp_scd\":%.1f,"
      "\"hum_scd\":%.1f,"
      "\"temp_bme\":%.1f,"
      "\"hum_bme\":%.1f,"
      "\"press_hpa\":%.1f,"
      "\"gas_ohm\":%lu,"
      "\"iaq_index\":%.1f,"
      "\"iaq_status\":\"%s\","
      "\"co2_status\":\"%s\"}",
      now,
      (unsigned)lastCO2,
      isnan(lastT_scd)?0.0f:lastT_scd,
      isnan(lastRH_scd)?0.0f:lastRH_scd,
      isnan(t_bme)?0.0f:t_bme,
      isnan(rh_bme)?0.0f:rh_bme,
      isnan(hPa)?0.0f:hPa,
      (unsigned long)(isnan(gas)?0:gas),
      isnan(gIdx)?50.0f:gIdx,
      iaqStatus,
      co2Status
    );

    // Publish telemetry (QoS1, not retained)
    bool ok = mqtt.publish(TOP_TELEMETRY, payload, false);
    if (!ok) {
      Serial.println("MQTT publish failed (telemetry), will retry next cycle.");
    }

    // Light heartbeat text (optional)
    mqtt.publish(TOP_HEARTBEAT, "beat", false);

    // Also print CSV-like line to USB serial for debug (optional)
    Serial.print(now); Serial.print(',');
    Serial.print(lastCO2); Serial.print(',');
    Serial.print(isnan(lastT_scd)?0.0f:lastT_scd,1); Serial.print(',');
    Serial.print(isnan(lastRH_scd)?0.0f:lastRH_scd,1); Serial.print(',');
    Serial.print(isnan(t_bme)?0.0f:t_bme,1); Serial.print(',');
    Serial.print(isnan(rh_bme)?0.0f:rh_bme,1); Serial.print(',');
    Serial.print(isnan(hPa)?0.0f:hPa,1); Serial.print(',');
    Serial.print((unsigned long)(isnan(gas)?0:gas)); Serial.print(',');
    Serial.print(isnan(gIdx)?50.0f:gIdx,1); Serial.print(',');
    Serial.print(iaqStatus); Serial.print(',');
    Serial.println(co2Status);
  }

  delay(25);
}
