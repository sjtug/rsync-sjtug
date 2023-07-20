UPDATE revisions
SET status = 'stale'
FROM (SELECT revision
      FROM revisions
      WHERE repository in (SELECT id FROM repositories WHERE name = $1)
        AND status = $2
      ORDER BY revision DESC
      OFFSET $3) as revs
WHERE revisions.revision = revs.revision
RETURNING revisions.revision;
