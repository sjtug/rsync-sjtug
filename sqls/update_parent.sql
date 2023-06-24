WITH parents AS (SELECT filename, substring(filename for rposition(filename, convert_to('/', 'UTF8')) - 1) AS parent
                 FROM objects
                 WHERE revision = $1
                   AND filename LIKE '%/%'),
     parents_id AS (SELECT parents.filename, id
                    FROM objects
                             INNER JOIN parents ON parents.parent = objects.filename
                    WHERE revision = $1)
UPDATE objects
SET parent = parents_id.id
FROM parents_id
WHERE revision = $1
  AND objects.filename = parents_id.filename;
