#!/usr/bin/env bash
set -e

# Move to project directory
cd "$(dirname "$0")"

# Activate virtualenv (adjust path if needed)
source Scripts/activate

# Run Streamlit
streamlit run app.py \
  --server.address=127.0.0.1 \
  --server.port=8501 \
  --server.headless=true \
  --browser.gatherUsageStats=false
