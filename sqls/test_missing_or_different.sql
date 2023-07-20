SELECT objects.filename
FROM objects
         LEFT JOIN objects AS o ON objects.filename = o.filename AND o.revision = $2
WHERE objects.revision = $1
  AND (objects.filename = ANY ($3::bytea[]))
  AND (o.revision IS NULL OR objects.filename != o.filename OR objects.len != o.len OR
       objects.modify_time != o.modify_time OR objects.type != o.type OR objects.blake2b != o.blake2b OR
       objects.target != o.target);
