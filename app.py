import os
import json
import threading
import time
from datetime import datetime
from flask import Flask, jsonify
from azure.servicebus import ServiceBusClient

# ---- Environment (App Settings in Azure portal) ----
# SB_CONNECTION: Service Bus connection string with Listen rights
# SB_TOPIC_NAME: e.g., topic-digdxeraasazureprd001
# SB_SUBSCRIPTION_NAME: e.g., appservice-receiver
SB_CONNECTION = os.getenv("SB_CONNECTION")
TOPIC_NAME = os.getenv("SB_TOPIC_NAME", "topic-digdxeraasazureprd001")
SUBSCRIPTION_NAME = os.getenv("SB_SUBSCRIPTION_NAME", "appservice-receiver")

app = Flask(__name__)

_last_lock = threading.Lock()
_last_message = None
_stop_signal = False

def _parse_message(msg):
    """Return dict with body + meta from a ServiceBusReceivedMessage."""
    raw = b"".join([b for b in msg.body])
    try:
        body = json.loads(raw.decode("utf-8"))
    except Exception:
        body = {"_raw": raw.decode("utf-8", "ignore")}
    return {
        "body": body,
        "content_type": getattr(msg, "content_type", None),
        "subject": getattr(msg, "subject", None),
        "application_properties": getattr(msg, "application_properties", None),
        "enqueued_time_utc": getattr(msg, "enqueued_time_utc", None),
        "received_at": datetime.utcnow().isoformat() + "Z"
    }

def _receiver_loop():
    global _last_message
    if not (SB_CONNECTION and TOPIC_NAME and SUBSCRIPTION_NAME):
        app.logger.error("Missing SB env vars. Set SB_CONNECTION, SB_TOPIC_NAME, SB_SUBSCRIPTION_NAME.")
        return
    while not _stop_signal:
        try:
            with ServiceBusClient.from_connection_string(SB_CONNECTION) as client:
                with client.get_subscription_receiver(
                    topic_name=TOPIC_NAME,
                    subscription_name=SUBSCRIPTION_NAME,
                    max_wait_time=5
                ) as receiver:
                    app.logger.info("ServiceBus receiver running on %s/%s", TOPIC_NAME, SUBSCRIPTION_NAME)
                    while not _stop_signal:
                        messages = receiver.receive_messages(max_message_count=10, max_wait_time=5)
                        if not messages:
                            continue
                        for msg in messages:
                            try:
                                parsed = _parse_message(msg)
                                with _last_lock:
                                    _last_message = parsed
                                app.logger.info("Received SB message: %s", json.dumps(parsed)[:1000])
                                receiver.complete_message(msg)
                            except Exception as e:
                                app.logger.exception("Error processing message: %s", e)
        except Exception as e:
            app.logger.exception("Receiver loop error, retrying in 5s: %s", e)
            time.sleep(5)

# Start background consumer thread on startup
threading.Thread(target=_receiver_loop, daemon=True).start()

@app.route("/", methods=["GET"])
def home():
    return "Receiver up. It subscribes to Service Bus and exposes GET /last."

@app.route("/last", methods=["GET"])
def last():
    with _last_lock:
        if _last_message is None:
            return jsonify({"message": "No messages received yet"}), 404
        return jsonify(_last_message)
