WITH ns AS (SELECT id FROM repositories WHERE name = $1),
     live_revisions AS (SELECT revision
                        from revisions
                        WHERE repository in (SELECT id FROM ns)
                          and status = 'live'),
     partial_revisions AS (SELECT revision
                           from revisions
                           WHERE repository in (SELECT id FROM ns)
                             and status = 'partial'),
     stale_revisions AS (SELECT revision
                         from revisions
                         WHERE repository in (SELECT id FROM ns)
                           and status = 'stale'),
     live_filenames AS (SELECT blake2b
                        from objects
                        WHERE revision IN (SELECT revision FROM live_revisions)
                          AND blake2b IS NOT NULL),
     partial_filenames AS (SELECT blake2b
                           from objects
                           WHERE revision IN (SELECT revision FROM partial_revisions)
                             AND blake2b IS NOT NULL),
     stale_filenames AS (SELECT blake2b
                         from objects
                         WHERE revision IN (SELECT revision FROM stale_revisions)
                           AND blake2b IS NOT NULL)
SELECT DISTINCT s.blake2b as "blake2b!"
FROM stale_filenames AS s
         LEFT JOIN live_filenames AS l
                   ON s.blake2b = l.blake2b
         LEFT JOIN partial_filenames AS p
                   ON s.blake2b = p.blake2b
WHERE l.blake2b IS NULL
  AND p.blake2b IS NULL;
