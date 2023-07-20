WITH ns AS
             (SELECT id FROM repositories WHERE name = $1),
     local_revs AS
         (SELECT revision from revisions WHERE repository IN (SELECT id FROM ns) AND status in ('live', 'partial'))
SELECT fl.idx, o.blake2b
FROM rsync_filelist AS fl
         LEFT JOIN objects AS o
                   ON o.revision IN (SELECT revision FROM local_revs)
                       AND fl.filename = o.filename
WHERE is_regular(fl.mode)
  AND o.filename IS NULL;
