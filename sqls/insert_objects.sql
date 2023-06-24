INSERT INTO objects(revision, filename, len, modify_time, type, blake2b, target)
SELECT *
FROM UNNEST(array_fill($1::INTEGER, $2), $3::BYTEA[], $4::BIGINT[], $5::TIMESTAMPTZ[], $6::filetype[], $7::BYTEA[],
            $8::BYTEA[])