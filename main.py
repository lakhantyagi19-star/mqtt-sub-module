import os, json, asyncio, ssl
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
from azure.iot.device.aio import IoTHubModuleClient
from azure.iot.device import Message

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "sensors/+/temperature")
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
MQTT_TLS = os.getenv("MQTT_TLS", "false").lower() == "true"

THRESHOLD = float(os.getenv("DEFAULT_THRESHOLD", "30"))
DEVICE_ID_ENV = "IOTEDGE_DEVICEID"

class Bridge:
    def __init__(self):
        self.loop = asyncio.get_event_loop()
        self.client = None
        self.edge = None

    async def init_edge(self):
        # This connects the module to EdgeHub (the local broker inside IoT Edge)
        self.edge = IoTHubModuleClient.create_from_edge_environment()
        await self.edge.connect()
        # Pull current desired props and listen for live patches
        twin = await self.edge.get_twin()
        self.apply_threshold(twin.get("desired", {}))
        def on_patch(patch):
            self.apply_threshold(patch)
        self.edge.on_twin_desired_properties_patch_received = on_patch

    def apply_threshold(self, desired):
        global THRESHOLD
        if "threshold" in desired:
            try:
                THRESHOLD = float(desired["threshold"])
                print(f"[Twin] threshold -> {THRESHOLD}")
            except Exception as e:
                print(f"[Twin] bad threshold: {e}")

    def start_mqtt(self):
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        if MQTT_TLS:
            self.client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
        if MQTT_USERNAME or MQTT_PASSWORD:
            self.client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

        def on_connect(client, userdata, flags, reason_code, properties=None):
            print("[MQTT] connected:", reason_code)
            client.subscribe(MQTT_TOPIC, qos=1)
        self.client.on_connect = on_connect

        def on_message(client, userdata, msg):
            try:
                body = json.loads(msg.payload.decode("utf-8"))
                temp = float(body.get("temperature", -1))
                if temp > THRESHOLD:
                    out = {
                        "device": body.get("device") or os.getenv(DEVICE_ID_ENV, "edge-gw"),
                        "temperature": temp,
                        "topic": msg.topic,
                        "ts": datetime.now(timezone.utc).isoformat()
                    }
                    m = Message(json.dumps(out))
                    # Set content type so IoT Hub treats body as JSON if you later route on body
                    m.content_type = "application/json"
                    m.content_encoding = "utf-8"
                    asyncio.run_coroutine_threadsafe(
                        self.edge.send_message_to_output(m, "hotOut"),
                        self.loop
                    )
            except Exception as e:
                print("[MQTT] parse/filter error:", e)

        self.client.on_message = on_message
        self.client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        self.client.loop_start()

    async def run(self):
        await self.init_edge()
        self.start_mqtt()
        while True:
            await asyncio.sleep(60)

if __name__ == "__main__":
    br = Bridge()
    asyncio.run(br.run())
