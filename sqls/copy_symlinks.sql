INSERT INTO objects(revision, filename, len, modify_time, type, target)
SELECT $1, filename, len, modify_time, 'symlink', target
FROM rsync_filelist
WHERE is_symlink(mode);
