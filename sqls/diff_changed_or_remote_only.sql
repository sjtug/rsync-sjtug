WITH ns AS
             (SELECT id FROM repositories WHERE name = $1),
     local_revs AS
         (SELECT revision from revisions WHERE repository IN (SELECT id FROM ns) AND status in ('live', 'partial'))
SELECT fl.idx,
       -- return blake2b with matching filename (highest revision)
       (SELECT o.blake2b
        FROM objects AS o
        WHERE o.revision IN (SELECT revision FROM local_revs)
          AND fl.filename = o.filename
        ORDER BY o.revision DESC
        LIMIT 1) as blake2b
FROM rsync_filelist as fl
         -- left join (exclude) fl rows with existing filename, matching mtime and len in objects
         LEFT JOIN objects AS o
                   ON o.revision IN (SELECT revision FROM local_revs)
                       AND fl.filename = o.filename
                       AND fl.modify_time = o.modify_time
                       AND fl.len = o.len
                       AND o.type = 'regular'
WHERE is_regular(mode)
  AND o.filename IS NULL;
