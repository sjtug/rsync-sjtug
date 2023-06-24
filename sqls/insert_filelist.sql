INSERT INTO rsync_filelist(filename, len, modify_time, mode, target, idx)
SELECT *
FROM UNNEST($1::bytea[], $2::BIGINT[], $3::TIMESTAMPTZ[], $4::INT[], $5::bytea[], $6::INT[])
ON CONFLICT (idx) DO UPDATE SET filename    = EXCLUDED.filename,
                                len         = EXCLUDED.len,
                                modify_time = EXCLUDED.modify_time,
                                mode        = EXCLUDED.mode,
                                target      = EXCLUDED.target
