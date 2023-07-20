SELECT cron.unschedule('nightly-stats-refresh');

DROP EXTENSION IF EXISTS pg_cron CASCADE;

DROP MATERIALIZED VIEW IF EXISTS revision_stats CASCADE;

DROP TRIGGER IF EXISTS revisions_change ON revisions CASCADE;

DROP FUNCTION IF EXISTS revisions_change_notify() CASCADE;

DROP FUNCTION IF EXISTS rposition(bytes bytea, pattern bytea) CASCADE;

DROP FUNCTION IF EXISTS is_symlink(mode integer) CASCADE;
DROP FUNCTION IF EXISTS is_directory(mode integer) CASCADE;
DROP FUNCTION IF EXISTS is_regular(mode integer) CASCADE;

DROP TABLE IF EXISTS objects CASCADE;
DROP TABLE IF EXISTS revisions CASCADE;
DROP TABLE IF EXISTS repositories CASCADE;

DROP TYPE IF EXISTS revision_status CASCADE;
DROP TYPE IF EXISTS filetype CASCADE;