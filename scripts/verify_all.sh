mkdir -p .github/workflows
cat > .github/workflows/certus-verify.yml <<'YAML'
name: Certus Verify

on:
  workflow_dispatch:
  schedule:
    - cron: "0 3 * * *"  # 03:00 UTC daily

jobs:
  verify:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install deps
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Create .env if missing (no secrets)
        run: |
          if [ ! -f .env ]; then
            echo "CERTUS_PAUSED=false" > .env
          fi

      - name: Run verification
        run: bash scripts/verify_all.sh
YAML
cat > .github/workflows/certus-verify.yml <<'YAML'
name: Certus Verify

on:
  workflow_dispatch:
  schedule:
    - cron: "0 3 * * *"  # 03:00 UTC daily

jobs:
  verify:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install deps (skip torch in CI)
        run: |
          python -m pip install --upgrade pip
          sed -E '/^torch==/d' requirements.txt > req-ci.txt
          pip install -r req-ci.txt

      - name: Create .env if missing (no secrets)
        run: |
          [ -f .env ] || echo "CERTUS_PAUSED=false" > .env

      - name: Run verification (CI-light)
        run: bash scripts/ci_verify.sh
YAML
