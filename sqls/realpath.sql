SELECT type as "type: _", blake2b, target
FROM objects
WHERE revision = $1
  AND filename = $2;
