#!/usr/bin/env bash
set -Eeuo pipefail

ts(){ date +"%Y-%m-%d %H:%M:%S"; }
LOG="logs/p29_$(date +%Y%m%d_%H%M%S).log"; mkdir -p logs

on_err() {
  code=$?
  echo -e "\n[$(ts)] ❌ Error (exit $code) at line $BASH_LINENO. Log: $LOG"
  echo "Keeping terminal open. Try: tail -n 200 $LOG"
  exec bash -i
}
trap on_err ERR

# Make repo importable
# removed: now installed as package export PYTHONPATH="$(pwd):${PYTHONPATH:-}"
echo "PYTHONPATH=$PYTHONPATH" | tee -a "$LOG"

# Normalize line endings
find scripts -type f -name "*.sh" -exec sed -i 's/\r$//' {} \; || true
find certus -type f -name "*.py" -exec sed -i 's/\r$//' {} \; || true
chmod +x scripts/*.sh 2>/dev/null || true

echo "[$(ts)] == RUN P2.9 ==" | tee -a "$LOG"

if [ ! -f data/markets.duckdb ]; then
  echo "[$(ts)] WARNING: data/markets.duckdb missing. Run: python scripts/fetch_markets.py" | tee -a "$LOG"
fi

{
  set -x
  python scripts/calc_indicators.py
  python scripts/calc_scores.py
  python scripts/calc_signals.py
  set +x
} 2>&1 | tee -a "$LOG"

python - <<'PY' 2>&1 | tee -a "$LOG" || true
import duckdb
con = duckdb.connect("data/markets.duckdb")
def show(q):
    try:
        print(q); print(con.sql(q).fetchdf(), "\n")
    except Exception as e:
        print("  [sample error]", e, "\n")
show("""SELECT symbol, ts, ROUND(price,4) price,
              ROUND(ema_9,4) ema9, ROUND(ema_20,4) ema20,
              ROUND(macd,6) macd, ROUND(macd_signal,6) macd_sig,
              ROUND(rsi_14,2) rsi
       FROM indicators ORDER BY ts DESC LIMIT 10""")
show("""SELECT symbol, ts, ROUND(price,4) price,
              ROUND(trend_score,3) score
       FROM scores ORDER BY ts DESC LIMIT 10""")
show("""SELECT symbol, ts, signal, ROUND(price,4) price
       FROM signals ORDER BY ts DESC LIMIT 10""")
con.close()
PY

echo "[$(ts)] ✅ DONE. Log: $LOG"
echo "Press ENTER to keep this terminal open; type 'exit' to close."
read -r _
