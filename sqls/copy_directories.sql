INSERT INTO objects(revision, filename, len, modify_time, type)
SELECT $1, filename, len, modify_time, 'directory'
FROM rsync_filelist
WHERE is_directory(mode);
