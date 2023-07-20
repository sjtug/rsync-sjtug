SELECT filename, len, type as "type: _", modify_time
FROM objects
WHERE revision = $1
  AND objects.parent IS NULL
ORDER BY type != 'directory', filename;
