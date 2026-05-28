#!/bin/bash
# 30-minute log checker for signaler_lbank_v8_2.py
# Prints last 200 lines of each log, highlights errors and key events

cd /home/user/fffff

echo "========================================"
echo "LOG CHECK: $(date)"
echo "========================================"

# Bot process status
PID_FILE="bot.pid"
if [ -f "$PID_FILE" ]; then
    BOT_PID=$(cat "$PID_FILE")
    if kill -0 "$BOT_PID" 2>/dev/null; then
        echo "[STATUS] Bot is RUNNING (PID=$BOT_PID)"
    else
        echo "[STATUS] Bot is DEAD (last PID=$BOT_PID)"
    fi
else
    echo "[STATUS] No PID file found — bot may not be started"
fi

echo ""
echo "--- bot_stdout.log (last 100 lines) ---"
if [ -f "bot_stdout.log" ]; then
    tail -100 bot_stdout.log
else
    echo "(no bot_stdout.log yet)"
fi

echo ""
echo "--- log.txt (last 100 lines) ---"
if [ -f "log.txt" ]; then
    tail -100 log.txt
else
    echo "(no log.txt yet)"
fi

echo ""
echo "--- log_1m.txt (last 50 lines) ---"
if [ -f "log_1m.txt" ]; then
    tail -50 log_1m.txt
else
    echo "(no log_1m.txt yet)"
fi

echo ""
echo "--- bot_crash.log ---"
if [ -f "bot_crash.log" ]; then
    cat bot_crash.log
else
    echo "(no crash log yet)"
fi

echo ""
echo "--- ERROR/EXCEPTION scan ---"
for f in bot_stdout.log log.txt log_1m.txt; do
    if [ -f "$f" ]; then
        ERRORS=$(grep -i -E "error|exception|traceback|crash|critical|WARN|failed" "$f" | tail -30)
        if [ -n "$ERRORS" ]; then
            echo "[$f ERRORS/WARNINGS]:"
            echo "$ERRORS"
        fi
    fi
done

echo ""
echo "--- BUY/SELL signals (last 24h from log.txt) ---"
if [ -f "log.txt" ]; then
    grep -E "\[BUY\]|\[SELL\]|\[WATCH\]|\[POSSIBLE_BUY\]|force.buy|force.sell|PMA|GF\+" log.txt | tail -50
fi

echo "========================================"
echo "END LOG CHECK: $(date)"
echo "========================================"
