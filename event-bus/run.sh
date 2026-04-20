#!/bin/sh
set -e
CONFIG=/data/options.json
export HA_URL=$(python3 -c "import json; print(json.load(open('$CONFIG'))['ha_url'])")
export HA_TOKEN=$(python3 -c "import json; print(json.load(open('$CONFIG'))['ha_token'])")
export API_PORT=$(python3 -c "import json; print(json.load(open('$CONFIG'))['api_port'])")
export INTELLIGENCE_URL=$(python3 -c "import json; print(json.load(open('$CONFIG'))['intelligence_url'])")
export SPOTIFY_URL=$(python3 -c "import json; print(json.load(open('$CONFIG'))['spotify_url'])")
export HUE_URL=$(python3 -c "import json; print(json.load(open('$CONFIG'))['hue_url'])")
export SWITCHBOT_URL=$(python3 -c "import json; print(json.load(open('$CONFIG'))['switchbot_url'])")
export MQTT_HOST=$(python3 -c "import json; print(json.load(open('$CONFIG'))['mqtt_host'])")
export MQTT_PORT=$(python3 -c "import json; print(json.load(open('$CONFIG'))['mqtt_port'])")
export WATCHED_DOMAINS=$(python3 -c "import json; print(json.load(open('$CONFIG'))['watched_domains'])")
echo "[INFO] TARS Event Bus v1.0.0"
exec python3 /app/server.py