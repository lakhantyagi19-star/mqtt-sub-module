import os
import json
import signal
import asyncio
import threading
import time
from typing import Optional, Tuple

import paho.mqtt.client as mqtt
from azure.iot.device.aio import IoTHubModuleClient
from azure.iot.device import Message
from azure.iot.device import exceptions as iot_exceptions

# ========= Config via ENV (with defaults) =========
BROKER_HOST   = os.getenv("BROKER__HOST", "test.mosquitto.org")
BROKER_PORT   = int(os.getenv("BROKER__PORT", "1883"))
BROKER_USER   = os.getenv("BROKER__USERNAME", "") or None
BROKER_PASS   = os.getenv("BROKER__PASSWORD", "") or None
SUB_TOPIC     = os.getenv("SUB__TOPIC", "sensors/+/temperature")
KEEPALIVE     = int(os.getenv("MQTT_KEEPALIVE", "60"))
USE_TLS       = os.getenv("MQTT_TLS", "false").lower() in ("1", "true", "yes")

# Default threshold (can be overridden by Module Twin desired.properties.threshold)
DEFAULT_THRESHOLD = float(os.getenv("THRESHOLD", "30"))

# IMPORTANT: Make sure your route uses the same output name.
# Either set IOTHUB_OUTPUT=hotOut here or make your route point to "filtered".
IOTHUB_OUTPUT = os.getenv("IOTHUB_OUTPUT", "filtered")

RETRY_BACKOFF_SECS = [2, 4, 8, 16, 30]  # backoff for IoT Hub connect retries

# ========= Globals & helpers =========
_stop_event = threading.Event()

def log(*parts):
    print("[mqtt-sub]", *parts, flush=True)

def parse_payload(payload_bytes: bytes) -> Tuple[Optional[float], Optional[str], Optional[dict]]:
    """
    Returns (temperature_value, device_name, parsed_json_or_none).
    Accepts either:
      - raw numeric payload: b"59.2"
      - JSON with {"temperature": X} or {"temp": X}; optional {"device": "..."}
    """
    try:
        text = payload_bytes.decode("utf-8", errors="strict").strip()
    except UnicodeDecodeError:
        return None, None, None

    # Try raw numeric first
    try:
        return float(text), None, None
    except ValueError:
        pass

    # Try JSON
    try:
        data = json.loads(text)
        # Accept common keys
        raw = data.get("temperature", data.get("temp", None))
        if raw is None:
            return None, None, data
        try:
            val = float(raw)
        except Exception:
            return None, None, data
        dev = data.get("device")
        return val, dev, data
    except json.JSONDecodeError:
        return None, None, None


# ========= Bridge class =========
class MqttToIoTEdgeBridge:
    def __init__(self):
        self.mqtt_client: Optional[mqtt.Client] = None
        self.module_client: Optional[IoTHubModuleClient] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None   # <-- set later in run()
        self.threshold = DEFAULT_THRESHOLD

    # ----- IoT Edge (ModuleClient) -----
    async def init_edge(self):
        """Create ModuleClient from Edge environment and connect with retries.
           Also load Module Twin desired threshold and subscribe to patches."""
        try:
            self.module_client = IoTHubModuleClient.create_from_edge_environment()
        except Exception as e:
            log("ERROR: Not running inside IoT Edge environment or env not set. "
                "ModuleClient creation failed:", repr(e))
            self.module_client = None
            return

        for i, delay in enumerate([0] + RETRY_BACKOFF_SECS):
            try:
                if delay:
                    log(f"IoT Hub connect retry in {delay}s (attempt {i+1}) …")
                    await asyncio.sleep(delay)
                log("Connecting to IoT Hub via Edge Hub …")
                await self.module_client.connect()
                log("Connected to IoT Hub ✅")
                break
            except (iot_exceptions.ConnectionFailedError, iot_exceptions.ClientError, Exception) as e:
                log("IoT Hub connect failed:", repr(e))
        else:
            log("FATAL: Could not connect to IoT Hub after retries.")

        # Try to pull current twin & desired threshold
        try:
            twin = await self.module_client.get_twin()
            desired = twin.get("desired", {}) if isinstance(twin, dict) else {}
            self._apply_desired(desired)
        except Exception as e:
            log("WARNING: get_twin failed:", repr(e))

        # Subscribe to desired property patches
        def on_patch(patch):
            self._apply_desired(patch)
        if self.module_client:
            self.module_client.on_twin_desired_properties_patch_received = on_patch

    def _apply_desired(self, desired: dict):
        if not isinstance(desired, dict):
            return
        if "threshold" in desired:
            try:
                self.threshold = float(desired["threshold"])
                log(f"[Twin] threshold -> {self.threshold}")
            except Exception as e:
                log(f"[Twin] bad threshold value: {desired.get('threshold')} ({e})")

    async def send_to_iothub(self, body: dict):
        """Send a JSON message to IoT Hub output, if ModuleClient is available."""
        if not self.module_client:
            return
        try:
            msg = Message(json.dumps(body))
            msg.content_type = "application/json"
            msg.content_encoding = "utf-8"
            await self.module_client.send_message_to_output(msg, IOTHUB_OUTPUT)
        except Exception as e:
            log("WARNING: send_message_to_output failed:", repr(e))

    # ----- MQTT side -----
    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            log("MQTT connected ✅ rc=0. Subscribing to:", SUB_TOPIC)
            client.subscribe(SUB_TOPIC)
        else:
            log(f"MQTT connect failed rc={rc}")

    def _on_message(self, client, userdata, msg):
        val, dev, parsed = parse_payload(msg.payload)

        if val is None:
            # Helpful diagnostics:
            preview = msg.payload[:120]
            if parsed is None:
                log(f"Ignoring non-numeric/non-JSON payload on {msg.topic}: {preview!r}")
            else:
                log(f"Ignoring payload without numeric temperature on {msg.topic}: {parsed}")
            return

        # Filtering logic
        if val > self.threshold:
            data = {
                "topic": msg.topic,
                "value": val,
                "threshold": self.threshold,
                "device": dev,
                "ts": int(time.time()),
            }
            log("Exceeded → forwarding to IoT Hub:", data)

            # Schedule send on the actual running loop captured in run()
            if self.module_client and self.loop:
                asyncio.run_coroutine_threadsafe(self.send_to_iothub(data), self.loop)
            else:
                log("No ModuleClient/loop; printed only:", data)
        else:
            # Below threshold
            pass

    def _on_disconnect(self, client, userdata, rc):
        if rc != 0:
            log("MQTT disconnected unexpectedly. rc=", rc)

    def start_mqtt(self):
        """Create and start the MQTT client in loop_start mode (non-blocking)."""
        client = mqtt.Client()  # v1 API constructor (works with paho-mqtt 1.x)

        if BROKER_USER and BROKER_PASS:
            client.username_pw_set(BROKER_USER, BROKER_PASS)

        if USE_TLS:
            client.tls_set()  # Use system CAs
            log("TLS enabled for MQTT")

        client.on_connect = self._on_connect
        client.on_message = self._on_message
        client.on_disconnect = self._on_disconnect

        client.connect(BROKER_HOST, BROKER_PORT, KEEPALIVE)
        client.loop_start()
        self.mqtt_client = client
        log(f"MQTT connecting to {BROKER_HOST}:{BROKER_PORT}, topic='{SUB_TOPIC}', "
            f"threshold={self.threshold}")

    def stop_mqtt(self):
        if self.mqtt_client is not None:
            try:
                self.mqtt_client.loop_stop()
                self.mqtt_client.disconnect()
            except Exception:
                pass

    # ----- Orchestration -----
    async def run(self):
        # Capture the *running* loop created by asyncio.run()
        self.loop = asyncio.get_running_loop()
        await self.init_edge()
        self.start_mqtt()
        while not _stop_event.is_set():
            await asyncio.sleep(0.5)
        self.stop_mqtt()
        if self.module_client:
            try:
                await self.module_client.shutdown()
            except Exception:
                pass


# ========= Entrypoint =========
def _handle_signal(signum, frame):
    log(f"Signal {signum} received → shutting down …")
    _stop_event.set()

signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

if __name__ == "__main__":
    log("Starting mqtt-sub module …")
    log(f"BROKER={BROKER_HOST}:{BROKER_PORT} TLS={USE_TLS} TOPIC='{SUB_TOPIC}' THRESHOLD={DEFAULT_THRESHOLD}")
    try:
        asyncio.run(MqttToIoTEdgeBridge().run())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        log("Fatal error:", repr(e))
    finally:
        log("Exited.")
