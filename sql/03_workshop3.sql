-- Workshop 3: rows read without a new observation, business update or rejection.
-- Runs on a fresh database or an existing Workshop 2 volume.
BEGIN;
SET LOCAL lock_timeout = '5s';

ALTER TABLE ingestion_run ADD COLUMN IF NOT EXISTS unchanged_count INTEGER NOT NULL DEFAULT 0;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_attribute
        WHERE attrelid = 'ingestion_run'::regclass AND attname = 'unchanged_count'
          AND atttypid = 'integer'::regtype AND attnotnull AND atthasdef AND NOT attisdropped
    ) THEN
        RAISE EXCEPTION 'ingestion_run.unchanged_count must be a non-null integer with a default';
    END IF;
END;
$$;

-- Backfill successful Workshop 2 runs without rewriting already correct counts.
UPDATE ingestion_run
SET unchanged_count = read_count - accepted_count - updated_count - rejected_count
WHERE status = 'success'
  AND unchanged_count IS DISTINCT FROM read_count - accepted_count - updated_count - rejected_count;

COMMIT;
