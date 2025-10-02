# main.py
import os
import json
import asyncio
import ssl
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from azure.iot.device.aio import IoTHubModuleClient
from azure.iot.device import Message

# ----------------------------
# Config via environment vars
# ----------------------------
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "sensors/+/temperature")
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
MQTT_TLS = os.getenv("MQTT_TLS", "false").lower() == "true"

# Default threshold (can be overridden by module twin desired.properties.threshold)
THRESHOLD = float(os.getenv("DEFAULT_THRESHOLD", "30"))

# Edge DeviceId is injected by IoT Edge env
EDGE_DEVICE_ID = os.getenv("IOTEDGE_DEVICEID", "edge-gw")

PRINT_PREFIX = "[mqtt-sub]"

def _log(msg: str):
    print(f"{PRINT_PREFIX} {msg}", flush=True)


class Bridge:
    """
    Subscribes to MQTT -> filters on temperature -> emits to EdgeHub output 'hotOut'
    Twin desired.properties.threshold controls the threshold at runtime.
    """
    def __init__(self):
        self.loop = asyncio.get_event_loop()
        self.edge_client: IoTHubModuleClient | None = None
        self.mqtt_client: mqtt.Client | None = None
        self.threshold = THRESHOLD

    # ------------- Edge (IoT Hub) side -------------
    async def init_edge(self):
        self.edge_client = IoTHubModuleClient.create_from_edge_environment()
        await self.edge_client.connect()
        _log("Connected to IoT Hub via Edge Hub ✅")

        # Pull current twin + subscribe to patches
        twin = await self.edge_client.get_twin()
        self._apply_desired(twin.get("desired", {}))

        def on_patch(patch):
            # Called on desired property updates
            self._apply_desired(patch)

        self.edge_client.on_twin_desired_properties_patch_received = on_patch

    def _apply_desired(self, desired: dict):
        if "threshold" in desired:
            try:
                self.threshold = float(desired["threshold"])
                _log(f"[Twin] threshold -> {self.threshold}")
            except Exception as e:
                _log(f"[Twin] bad threshold value: {desired.get('threshold')} ({e})")

    async def send_hot(self, body: dict):
        """
        Sends one JSON message to output 'hotOut' (which should be routed to $upstream).
        """
        if not self.edge_client:
            _log("Edge client not ready; dropping message")
            return
        try:
            msg = Message(json.dumps(body))
            msg.content_type = "application/json"
            msg.content_encoding = "utf-8"
            await self.edge_client.send_message_to_output(msg, "hotOut")
            _log(f"Sent HOT event upstream: {body}")
        except Exception as e:
            _log(f"Failed to send message to output 'hotOut': {e}")

    # ------------- MQTT side -------------
    def start_mqtt(self):
        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

        # Auth / TLS if provided
        if MQTT_TLS:
            # NOTE: for public brokers you may need to provide CA if verification fails
            self.mqtt_client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
        if MQTT_USERNAME or MQTT_PASSWORD:
            self.mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

        def on_connect(client, userdata, flags, reason_code, properties=None):
            _log(f"MQTT connected rc={reason_code}. Subscribing to: {MQTT_TOPIC}")
            client.subscribe(MQTT_TOPIC, qos=1)

        def on_message(client, userdata, msg):
            """
            Decode bytes -> JSON -> validate numeric temperature -> filter -> async send to Edge
            """
            try:
                payload_str = msg.payload.decode("utf-8", errors="strict")
                data = json.loads(payload_str)

                # Accept common keys: "temperature" or "temp"
                raw_temp = data.get("temperature", data.get("temp", None))
                if raw_temp is None:
                    _log(f"Ignoring payload without 'temperature' key on {msg.topic}: {payload_str}")
                    return

                # Coerce to float safely
                try:
                    temp = float(raw_temp)
                except Exception:
                    _log(f"Ignoring non-numeric temperature on {msg.topic}: {payload_str}")
                    return

                # Pass only 'hot' messages
                if temp > self.threshold:
                    out = {
                        "device": data.get("device") or EDGE_DEVICE_ID,
                        "temperature": temp,
                        "topic": msg.topic,
                        "ts": datetime.now(timezone.utc).isoformat()
                    }
                    # Send asynchronously into EdgeHub output hotOut
                    asyncio.run_coroutine_threadsafe(self.send_hot(out), self.loop)
                # else: silently drop cool messages

            except json.JSONDecodeError:
                _log(f"Ignoring invalid JSON on {msg.topic}: {msg.payload!r}")
            except UnicodeDecodeError:
                _log(f"Ignoring non-UTF8 bytes on {msg.topic}")
            except Exception as e:
                _log(f"Unexpected error processing MQTT message: {e}")

        self.mqtt_client.on_connect = on_connect
        self.mqtt_client.on_message = on_message

        _log(f"BROKER={MQTT_HOST}:{MQTT_PORT} TLS={MQTT_TLS} "
             f"TOPIC='{MQTT_TOPIC}' THRESHOLD={self.threshold}")
        self.mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        self.mqtt_client.loop_start()

    # ------------- Main run loop -------------
    async def run(self):
        await self.init_edge()
        self.start_mqtt()
        # Keep process alive
        while True:
            await asyncio.sleep(60)


if __name__ == "__main__":
    try:
        bridge = Bridge()
        asyncio.run(bridge.run())
    except KeyboardInterrupt:
        _log("Shutting down...")
