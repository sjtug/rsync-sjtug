WITH parent AS (SELECT id FROM objects WHERE filename = NULLIF($1, ''::bytea) AND revision = $2)
SELECT filename, len, type as "type: _", modify_time
FROM objects
WHERE revision = $2
  AND objects.parent = (SELECT id FROM parent)
ORDER BY type != 'directory', filename;
