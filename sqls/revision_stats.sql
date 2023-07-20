-- revision stats page
WITH revs AS (SELECT revision, status, created_at, completed_at
              FROM revisions
              WHERE repository = (SELECT id FROM repositories WHERE name = $1))
SELECT revs.revision, status AS "status: _", created_at, completed_at - created_at AS "elapsed", count, sum
FROM revs
         LEFT JOIN revision_stats ON revs.revision = revision_stats.revision
WHERE revs.status != 'stale'
ORDER BY revs.revision;
