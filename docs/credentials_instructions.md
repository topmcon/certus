# Credentials testing & safe storage

This file explains how to safely test API credentials locally and store only non-sensitive results in the repository.

Key points
- Never commit secret values (API keys, passwords, tokens) into the repo.
- The repository contains `scripts/test_external_apis.py` which performs lightweight, read-only checks for the services used by this codebase. The script reads credentials from environment variables and will skip tests when env vars are not set.
- After running tests locally, the script prints OK/FAIL/SKIP results. You can then update `docs/credentials_inventory.json` (already present) with any additional notes — do not add secret values.

How to run tests locally (example)
1. Open a terminal in the repo root.
2. Export only the secrets you want to test into the shell (they remain in your shell session only):

```bash
export COINGECKO_API_KEY="<your-coingecko-key>"
export FINNHUB_API_KEY="<your-finnhub-key>"
export ALPHAVANTAGE_API_KEY="<your-alphavantage-key>"
export CRYPTOPANIC_API_KEY="<your-cryptopanic-key>"
export COINMARKETCAL_API_KEY="<your-coinmarketcal-key>"
# add more as needed
```

3. Run the test script:

```bash
python scripts/test_external_apis.py
```

4. The script prints results. If you want to save a non-sensitive summary to the repo, open `docs/credentials_inventory.json` and update the `tested`/`status` fields and `note` as appropriate. Do NOT include any secret values.

Storing secrets for CI
- Use GitHub Actions secrets (Repository Settings → Secrets) and reference them in workflows. Example (in your workflow yaml):

```yaml
env:
  COINGECKO_API_KEY: ${{ secrets.COINGECKO_API_KEY }}
```

Recommended cleanup if secrets leaked
- If any real keys were accidentally committed, rotate them immediately at the provider.
- Remove the committed secret from the repo index and add to `.gitignore`:

```bash
git rm --cached .env
echo ".env" >> .gitignore
git commit -m "Remove .env from repo and ignore it"
```

If the secret is present in commit history, consider rewriting history using `git filter-repo` or BFG and then rotating keys.

If you want me to run the tests here using keys you provided, I will not commit or print secret values — but I will not store secrets in the repo. Instead I'll record only the pass/fail results in `docs/credentials_inventory.json` (no secret values). If you want me to proceed running tests here, confirm and I will run them now.
