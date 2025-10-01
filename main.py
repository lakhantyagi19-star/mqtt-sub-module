import os
import json
import signal
import asyncio
import threading
import time
from typing import Optional

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
THRESHOLD     = float(os.getenv("THRESHOLD", "30"))
IOTHUB_OUTPUT = os.getenv("IOTHUB_OUTPUT", "filtered")

RETRY_BACKOFF_SECS = [2, 4, 8, 16, 30]  # backoff for IoT Hub connect retries

# ========= Globals & helpers =========
_stop_event = threading.Event()


def log(*parts):
    print("[mqtt-sub]", *parts, flush=True)


def to_float(payload_bytes: bytes) -> Optional[float]:
    try:
        s = payload_bytes.decode("utf-8", errors="ignore").strip()
        return float(s)
    except Exception:
        return None


# ========= Bridge class =========
class MqttToIoTEdgeBridge:
    def __init__(self):
        self.mqtt_client: Optional[mqtt.Client] = None
        self.module_client: Optional[IoTHubModuleClient] = None
        self.loop = asyncio.get_event_loop()

    # ----- IoT Edge (ModuleClient) -----
    async def init_edge(self):
        """Create ModuleClient from Edge environment and connect with retries."""
        # Create from edge environment. This only works when running **as an IoT Edge module**.
        try:
            self.module_client = IoTHubModuleClient.create_from_edge_environment()
        except Exception as e:
            log("ERROR: Not running inside IoT Edge environment or env not set. "
                "ModuleClient creation failed:", repr(e))
            self.module_client = None
            return

        # Retry connect with backoff
        for i, delay in enumerate([0] + RETRY_BACKOFF_SECS):
            try:
                if delay:
                    log(f"IoT Hub connect retry in {delay}s (attempt {i+1}) …")
                    await asyncio.sleep(delay)
                log("Connecting to IoT Hub via Edge Hub …")
                await self.module_client.connect()
                log("Connected to IoT Hub ✅")
                return
            except (iot_exceptions.ConnectionFailedError, iot_exceptions.ClientError, Exception) as e:
                log("IoT Hub connect failed:", repr(e))
        log("FATAL: Could not connect to IoT Hub after retries.")
        # Keep running anyway — we can still log locally; messages will be skipped.

    async def send_to_iothub(self, body: dict):
        """Send a JSON message to IoT Hub output, if ModuleClient is available."""
        if not self.module_client:
            return  # running without Edge or connection failed
        try:
            msg = Message(json.dumps(body))
            msg.content_type = "application/json"
            msg.content_encoding = "utf-8"
            await self.module_client.send_message_to_output(msg, IOTHUB_OUTPUT)
        except Exception as e:
            log("WARNING: send_message_to_output failed:", repr(e))

    # ----- MQTT side -----
    def _on_connect(self, client, userdata, flags, rc):
        # v1 API signature: rc is int (0=success)
        if rc == 0:
            log("MQTT connected ✅ rc=0. Subscribing to:", SUB_TOPIC)
            client.subscribe(SUB_TOPIC)
        else:
            log(f"MQTT connect failed rc={rc}")

    def _on_message(self, client, userdata, msg):
        val = to_float(msg.payload)
        if val is None:
            log(f"Ignoring non-numeric payload on {msg.topic}: {msg.payload[:64]!r}")
            return

        # Filtering logic
        if val > THRESHOLD:
            data = {
                "topic": msg.topic,
                "value": val,
                "threshold": THRESHOLD,
                "ts": int(time.time()),
            }
            log("Exceeded → forwarding to IoT Hub:", data)

            # Use asyncio thread-safe call to send via ModuleClient
            if self.module_client:
                asyncio.run_coroutine_threadsafe(self.send_to_iothub(data), self.loop)
            else:
                # No IoT Edge binding; just print
                log("No ModuleClient; printed only:", data)
        else:
            # Below threshold; ignore (or log verbosely if you like)
            pass

    def _on_disconnect(self, client, userdata, rc):
        # rc != 0 means unexpected disconnect
        if rc != 0:
            log("MQTT disconnected unexpectedly. rc=", rc)

    def start_mqtt(self):
        """Create and start the MQTT client in loop_start mode (non-blocking)."""
        client = mqtt.Client()  # v1 constructor; DO NOT use CallbackAPIVersion (that’s v2 only)

        if BROKER_USER and BROKER_PASS:
            client.username_pw_set(BROKER_USER, BROKER_PASS)

        if USE_TLS:
            # Basic TLS (you can extend with CA/certs via more env vars if needed)
            client.tls_set()  # Use system CAs
            log("TLS enabled for MQTT")

        client.on_connect = self._on_connect
        client.on_message = self._on_message
        client.on_disconnect = self._on_disconnect

        # Connect and start loop
        client.connect(BROKER_HOST, BROKER_PORT, KEEPALIVE)
        client.loop_start()
        self.mqtt_client = client
        log(f"MQTT connecting to {BROKER_HOST}:{BROKER_PORT}, topic='{SUB_TOPIC}', "
            f"threshold={THRESHOLD}")

    def stop_mqtt(self):
        if self.mqtt_client is not None:
            try:
                self.mqtt_client.loop_stop()
                self.mqtt_client.disconnect()
            except Exception:
                pass

    # ----- Orchestration -----
    async def run(self):
        # 1) Initialize IoT Edge client (non-fatal if missing)
        await self.init_edge()

        # 2) Start MQTT
        self.start_mqtt()

        # 3) Wait for stop signal
        while not _stop_event.is_set():
            await asyncio.sleep(0.5)

        # 4) Cleanup
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
    log(f"BROKER={BROKER_HOST}:{BROKER_PORT} TLS={USE_TLS} TOPIC='{SUB_TOPIC}' THRESHOLD={THRESHOLD}")
    try:
        asyncio.run(MqttToIoTEdgeBridge().run())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        log("Fatal error:", repr(e))
    finally:
        log("Exited.")
