#!/bin/zsh

# ===== CONFIG =====
SOURCE="/Volumes/External_SSD/Master_test"
DEST="/Volumes/Photo/Master_test"
LOG_DIR="$HOME/backup_logs"
DATE=$(date +"%Y-%m-%d_%H-%M-%S")
LOG_FILE="$LOG_DIR/rsync_$DATE.log"

# ===== PRE-CHECKS =====
mkdir -p "$LOG_DIR"

if [ ! -d "$SOURCE" ]; then
  osascript -e 'display notification "External SSD not mounted" with title "Backup FAILED"'
  exit 1
fi

if [ ! -d "$(dirname "$DEST")" ]; then
  osascript -e 'display notification "NAS not mounted" with title "Backup FAILED"'
  exit 1
fi

# ===== RUN RSYNC =====
rsync -avh \
  --delete-after \
  --progress \
  --stats \
  --exclude=".DS_Store" \
  --exclude="*.lrcat.lock" \
  "$SOURCE/" \
  "$DEST/" >> "$LOG_FILE" 2>&1

STATUS=$?

# ===== NOTIFICATION =====
if [ $STATUS -eq 0 ]; then
  osascript -e 'display notification "SSD → NAS backup completed successfully" with title "Backup OK"'
else
  osascript -e 'display notification "Backup failed — check logs" with title "Backup FAILED"'
fi

exit $STATUS
