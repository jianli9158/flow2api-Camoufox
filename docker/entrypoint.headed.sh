#!/bin/sh
set -eu

DISPLAY_VALUE="${DISPLAY:-:99}"
XVFB_SCREEN_VALUE="${XVFB_SCREEN:-1440x900x24}"
export DISPLAY="${DISPLAY_VALUE}"

if [ "${ALLOW_DOCKER_HEADED_CAPTCHA:-true}" = "true" ] || [ "${ALLOW_DOCKER_HEADED_CAPTCHA:-1}" = "1" ]; then
    display_suffix="$(printf '%s' "${DISPLAY}" | sed 's/^://; s/\..*$//')"
    socket_path="/tmp/.X11-unix/X${display_suffix}"

    mkdir -p /tmp/.X11-unix
    rm -f "/tmp/.X${display_suffix}-lock"

    echo "[entrypoint] starting Xvfb on DISPLAY=${DISPLAY} (${XVFB_SCREEN_VALUE})"
    Xvfb "${DISPLAY}" -screen 0 "${XVFB_SCREEN_VALUE}" -ac +extension RANDR >/tmp/xvfb.log 2>&1 &

    waited=0
    while [ ! -S "${socket_path}" ] && [ "${waited}" -lt 100 ]; do
        sleep 0.1
        waited=$((waited + 1))
    done

    if [ ! -S "${socket_path}" ]; then
        echo "[entrypoint] failed to start Xvfb, socket not ready: ${socket_path}" >&2
        exit 1
    fi

    echo "[entrypoint] starting Fluxbox on DISPLAY=${DISPLAY}"
    fluxbox >/tmp/fluxbox.log 2>&1 &
fi

echo "[entrypoint] starting flow2api (headed Camoufox mode)"
if [ -n "${CAMOUFOX_PATH:-}" ] && [ -x "${CAMOUFOX_PATH}" ]; then
    echo "[entrypoint] Camoufox executable: ${CAMOUFOX_PATH}"
elif [ -x "/app/runtime/camoufox/camoufox" ]; then
    export CAMOUFOX_PATH="/app/runtime/camoufox/camoufox"
    echo "[entrypoint] Camoufox executable: ${CAMOUFOX_PATH}"
else
    echo "[entrypoint] Camoufox binary not pre-mounted; flow2api will auto-bootstrap Camoufox"
fi

exec python main.py
