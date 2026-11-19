-- Workshop 3: records read but not rewritten because their content hash did not change.
-- Safe to run more than once.
ALTER TABLE ingestion_run ADD COLUMN IF NOT EXISTS unchanged_count INTEGER NOT NULL DEFAULT 0;

-- Backfill: every read record is accepted, updated, unchanged or rejected
UPDATE ingestion_run
SET unchanged_count = read_count - accepted_count - updated_count - rejected_count
WHERE status = 'success';
