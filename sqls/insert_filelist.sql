INSERT INTO rsync_filelist(filename, len, modify_time, mode, target, idx)
SELECT *
FROM UNNEST($1::bytea[], $2::BIGINT[], $3::TIMESTAMPTZ[], $4::INT[], $5::bytea[], $6::INT[])
