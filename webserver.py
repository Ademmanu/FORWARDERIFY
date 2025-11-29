from flask import Flask, request, jsonify
import threading
import time
import os
import logging

logger = logging.getLogger("webserver")

app = Flask(__name__)

START_TIME = time.time()

# Default container limit (in MB). Can be overridden with CONTAINER_MAX_RAM_MB env.
DEFAULT_CONTAINER_MAX_RAM_MB = int(os.getenv("CONTAINER_MAX_RAM_MB", "512"))

# Cache the detected container limit to avoid repeated sysfs reads
_cached_container_limit_mb = None

# Monitoring callback set by forward.py; should be a callable returning serializable dict
_monitor_callback = None


def register_monitoring(callback):
    """
    Register a callable that will be used by the /metrics endpoint to report
    runtime metrics from the forwarding subsystem.

    The callback should be fast and return a JSON-serializable dict. It will be
    called in the Flask thread (so it must handle cross-thread reads safely).
    """
    global _monitor_callback
    _monitor_callback = callback
    logger.info("Monitoring callback registered")


def _mb_from_bytes(n_bytes: int) -> float:
    return round(n_bytes / (1024 * 1024), 2)


def _read_cgroup_memory_limit_bytes() -> int:
    candidates = [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ]

    for path in candidates:
        try:
            if not os.path.exists(path):
                continue
            with open(path, "r") as fh:
                raw = fh.read().strip()
            if raw == "max":
                return 0
            val = int(raw)
            if val <= 0:
                return 0
            if val > (1 << 50):
                return 0
            return val
        except Exception:
            continue

    try:
        with open("/proc/self/cgroup", "r") as fh:
            lines = fh.read().splitlines()
        for ln in lines:
            parts = ln.split(":")
            if len(parts) >= 3:
                controllers = parts[1]
                cpath = parts[2]
                if "memory" in controllers.split(","):
                    possible = f"/sys/fs/cgroup/memory{cpath}/memory.limit_in_bytes"
                    if os.path.exists(possible):
                        with open(possible, "r") as fh:
                            raw = fh.read().strip()
                        val = int(raw)
                        if val > 0 and val < (1 << 50):
                            return val
                    possible2 = f"/sys/fs/cgroup{cpath}/memory.max"
                    if os.path.exists(possible2):
                        with open(possible2, "r") as fh:
                            raw = fh.read().strip()
                        if raw != "max":
                            val = int(raw)
                            if val > 0 and val < (1 << 50):
                                return val
    except Exception:
        pass

    return 0


def get_container_memory_limit_mb() -> float:
    global _cached_container_limit_mb
    if _cached_container_limit_mb is not None:
        return _cached_container_limit_mb

    bytes_limit = _read_cgroup_memory_limit_bytes()
    if bytes_limit and bytes_limit > 0:
        _cached_container_limit_mb = _mb_from_bytes(bytes_limit)
    else:
        _cached_container_limit_mb = float(os.getenv("CONTAINER_MAX_RAM_MB", str(DEFAULT_CONTAINER_MAX_RAM_MB)))
    return _cached_container_limit_mb


@app.route("/", methods=["GET"])
def home():
    container_limit = get_container_memory_limit_mb()
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Bot Status</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                text-align: center;
                padding: 50px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
            }}
            .status {{
                background: rgba(255,255,255,0.1);
                padding: 30px;
                border-radius: 15px;
                max-width: 600px;
                margin: 0 auto;
                text-align: left;
            }}
            h1 {{ font-size: 2.2em; margin: 0; text-align: center; }}
            p {{ font-size: 1.0em; }}
            .emoji {{ font-size: 2.5em; text-align: center; }}
            .stats {{ font-family: monospace; margin-top: 12px; }}
        </style>
    </head>
    <body>
        <div class="status">
            <div class="emoji">🤖</div>
            <h1>Forwarder Bot Status</h1>
            <p>Bot is running. Use the monitoring endpoints:</p>
            <ul>
              <li>/health — basic uptime</li>
              <li>/webhook — simple webhook endpoint</li>
              <li>/metrics — forwarding subsystem metrics (if registered)</li>
            </ul>
            <div class="stats">
              <strong>Container memory limit (detected):</strong> {container_limit} MB
            </div>
        </div>
    </body>
    </html>
    """
    return html


@app.route("/health", methods=["GET"])
def health():
    uptime = int(time.time() - START_TIME)
    return jsonify({"status": "healthy", "uptime_seconds": uptime}), 200


@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    now = int(time.time())
    if request.method == "POST":
        data = request.get_json(silent=True)
        return jsonify({"status": "ok", "received": True, "timestamp": now, "data": data}), 200
    else:
        return jsonify({"status": "ok", "method": "GET", "timestamp": now}), 200


@app.route("/metrics", methods=["GET"])
def metrics():
    """
    Returns forwarding subsystem metrics if a callback has been registered by the forward process.
    The callback runs in the Flask thread; it must handle cross-thread reads safely.
    """
    if _monitor_callback is None:
        return jsonify({"status": "unavailable", "reason": "no monitor registered"}), 200

    try:
        data = _monitor_callback()
        return jsonify({"status": "ok", "metrics": data}), 200
    except Exception as e:
        logger.exception("Monitoring callback failed")
        return jsonify({"status": "error", "error": str(e)}), 500


def run_server():
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False, threaded=True)


def start_server_thread():
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    print("🌐 Web server started on port 5000")
