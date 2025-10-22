#!/usr/bin/env bash
set -euo pipefail

# ================================
# Certus Backup Helper (P4.2)
# - Saves work, pushes branch
# - Creates timestamped backup branch
# - Creates timestamped annotated tag
# - Zips repo (without .git) to backups/
# Usage:
#   ./scripts/backup.sh "Your commit message"
# (message optional; has a sensible default)
# ================================

log() { printf "\n\033[1;32m== %s\033[0m\n" "$*"; }
warn() { printf "\n\033[1;33m!! %s\033[0m\n" "$*"; }
err() { printf "\n\033[1;31mXX %s\033[0m\n" "$*"; exit 1; }

# 0) Sanity checks
command -v git >/dev/null 2>&1 || err "git not found in PATH"
git rev-parse --is-inside-work-tree >/dev/null 2>&1 || err "Not inside a git repository"

# 1) Context
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
ROOT="$(git rev-parse --show-toplevel)"
REPO_NAME="$(basename "$ROOT")"
CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
COMMIT_MSG="${1:-P4.2 checkpoint — auto snapshot $TIMESTAMP}"

BACKUP_BRANCH="backup/p4.2-stable-$TIMESTAMP"
TAG_NAME="v4.2-$TIMESTAMP"
ZIP_DIR="$ROOT/backups"
ZIP_PATH="$ZIP_DIR/${REPO_NAME}_p4.2_${TIMESTAMP}.zip"

log "Repo: $REPO_NAME"
log "Current branch: $CURRENT_BRANCH"

# 2) Add + commit
log "Staging all changes…"
git add -A

# Only commit if there is something to commit
if ! git diff --cached --quiet; then
  log "Committing changes: $COMMIT_MSG"
  git commit -m "$COMMIT_MSG"
else
  warn "No staged changes. Creating an empty checkpoint commit for traceability."
  git commit --allow-empty -m "$COMMIT_MSG"
fi

# 3) Push current branch
log "Pushing branch '$CURRENT_BRANCH' to origin…"
git push -u origin "$CURRENT_BRANCH"

# 4) Create & push backup branch
log "Creating backup branch: $BACKUP_BRANCH"
git branch "$BACKUP_BRANCH"
git push -u origin "$BACKUP_BRANCH"

# 5) Create & push annotated tag
log "Tagging snapshot: $TAG_NAME"
git tag -a "$TAG_NAME" -m "Certus P4.2 snapshot $TIMESTAMP"
git push origin "$TAG_NAME"

# 6) Zip snapshot (excluding .git and previous backups)
log "Creating zip snapshot at: $ZIP_PATH"
mkdir -p "$ZIP_DIR"
(
  cd "$ROOT"
  zip -rq "$ZIP_PATH" . -x ".git/*" "backups/*"
)

log "Backup complete ✅"
printf "\nCreated:\n"
printf "  • Backup branch: %s\n" "$BACKUP_BRANCH"
printf "  • Tag:           %s\n" "$TAG_NAME"
printf "  • Zip:           %s\n\n" "$ZIP_PATH"
