-- Add migration script here
CREATE TYPE filetype AS ENUM ('regular', 'directory', 'symlink');
CREATE TYPE revision_status as ENUM ('partial', 'live', 'stale');
CREATE TABLE repositories
(
    id   SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);
CREATE TABLE revisions
(
    revision   SERIAL PRIMARY KEY,
    repository INTEGER         NOT NULL REFERENCES repositories (id),
    created_at timestamptz(0)  NOT NULL,
    status     revision_status NOT NULL
);
CREATE INDEX revisions_repository_idx ON revisions (repository);
CREATE TABLE objects
(
    id          SERIAL         NOT NULL PRIMARY KEY,
    revision    INTEGER        NOT NULL REFERENCES revisions (revision),
    filename    bytea          NOT NULL,
    parent      INTEGER REFERENCES objects (id),
    len         BIGINT         NOT NULL,
    modify_time timestamptz(0) NOT NULL,
    type        filetype       NOT NULL,
    blake2b     bytea
        CONSTRAINT blake2b_len CHECK (length(blake2b) = 20),
    target      bytea,
    CONSTRAINT required_fields CHECK ( (type = 'regular' AND blake2b IS NOT NULL AND target IS NULL) OR
                                       (type = 'directory' AND blake2b IS NULL AND target IS NULL) OR
                                       (type = 'symlink' AND blake2b IS NULL AND target IS NOT NULL) )
);
CREATE INDEX objects_revision_filename_idx ON objects (revision, filename);
CREATE INDEX objects_parent_idx ON objects (revision, parent);

CREATE OR REPLACE FUNCTION rposition(bytes bytea, pattern bytea)
    RETURNS INTEGER AS
$pos$
BEGIN
    FOR i IN REVERSE length(bytes) - length(pattern) .. 0
        LOOP
            IF substring(bytes from i for length(pattern)) = pattern THEN
                RETURN i;
            END IF;
        END LOOP;
    RETURN NULL;
END;
$pos$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
