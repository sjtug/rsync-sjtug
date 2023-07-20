WITH ns AS
             (SELECT id FROM repositories WHERE name = $1),
     local_revs AS
         (SELECT revision from revisions WHERE repository IN (SELECT id FROM ns) AND status in ('live', 'partial'))
INSERT
INTO objects (revision, filename, len, modify_time, type, blake2b, target)
SELECT DISTINCT ON (o.filename) $2,
                                o.filename,
                                o.len,
                                o.modify_time,
                                o.type,
                                o.blake2b,
                                o.target
FROM rsync_filelist AS fl
         INNER JOIN objects AS o
                    ON o.revision IN (SELECT revision FROM local_revs)
                        AND fl.filename = o.filename
WHERE is_regular(mode)
  AND o.type = 'regular'
  AND fl.modify_time = o.modify_time
  AND fl.len = o.len
ORDER BY o.filename, o.revision DESC;
