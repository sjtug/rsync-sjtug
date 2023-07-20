CREATE UNLOGGED TABLE rsync_filelist
(
    idx         INTEGER PRIMARY KEY,
    filename    bytea          NOT NULL,
    len         BIGINT         NOT NULL,
    modify_time TIMESTAMPTZ(0) NOT NULL,
    mode        INTEGER        NOT NULL,
    target      bytea
);