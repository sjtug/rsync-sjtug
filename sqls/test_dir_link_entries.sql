SELECT objects.filename, objects.modify_time, objects.type as "type: FileType", objects.target
FROM objects
WHERE objects.revision = $1
    AND objects.type IN ('directory', 'symlink')
    AND (objects.filename = ANY ($2::bytea[]))
ORDER BY objects.filename
