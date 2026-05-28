#!/bin/bash
# Watchdog runner for signaler_lbank_v8_2.py
# Restarts the bot automatically on crash, logs all output to bot_stdout.log

cd /home/user/fffff

RESTART_DELAY=5
CRASH_LOG="bot_crash.log"
STDOUT_LOG="bot_stdout.log"
PID_FILE="bot.pid"

echo "[WATCHDOG] Starting bot watchdog at $(date)" | tee -a "$CRASH_LOG"

while true; do
    echo "[WATCHDOG] Launching bot at $(date)" | tee -a "$CRASH_LOG"
    python3 signaler_lbank_v8_2.py >> "$STDOUT_LOG" 2>&1 &
    BOT_PID=$!
    echo $BOT_PID > "$PID_FILE"
    echo "[WATCHDOG] Bot PID=$BOT_PID" | tee -a "$CRASH_LOG"
    wait $BOT_PID
    EXIT_CODE=$?
    echo "[WATCHDOG] Bot exited with code=$EXIT_CODE at $(date). Restarting in ${RESTART_DELAY}s..." | tee -a "$CRASH_LOG"
    sleep $RESTART_DELAY
done
