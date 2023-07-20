CREATE TYPE filetype AS ENUM ('regular', 'directory', 'symlink');
CREATE TYPE revision_status as ENUM ('partial', 'live', 'stale');
CREATE TABLE repositories
(
    id   INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);
CREATE TABLE revisions
(
    revision     INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    repository   INTEGER         NOT NULL REFERENCES repositories (id) ON DELETE CASCADE,
    created_at   timestamptz(0)  NOT NULL,
    completed_at timestamptz(0),
    status       revision_status NOT NULL,
    CONSTRAINT completed_unless_partial CHECK (status = 'partial' OR completed_at IS NOT NULL)
);
CREATE INDEX revisions_repository_idx ON revisions (repository);
CREATE TABLE objects
(
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    revision    INTEGER        NOT NULL REFERENCES revisions (revision) ON DELETE CASCADE,
    filename    bytea          NOT NULL,
    parent      BIGINT, -- this might have a REFERENCES objects(id) fkey, but it makes batch del EXTREMELY slow
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
CREATE UNIQUE INDEX objects_revision_filename_idx ON objects (revision, filename);
CREATE INDEX objects_parent_idx ON objects (revision, parent);

CREATE OR REPLACE FUNCTION is_regular(mode integer) RETURNS bool
    LANGUAGE SQL
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
-- RETURN (mode & 0o170000) = 0o100000;
RETURN (mode & 61440) = 32768;
CREATE OR REPLACE FUNCTION is_directory(mode integer) RETURNS bool
    LANGUAGE SQL
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
-- RETURN (mode & 0o170000) = 0o040000;
RETURN (mode & 61440) = 16384;
CREATE OR REPLACE FUNCTION is_symlink(mode integer) RETURNS bool
    LANGUAGE SQL
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
-- RETURN (mode & 0o170000) = 0o120000;
RETURN (mode & 61440) = 40960;

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
$pos$ LANGUAGE plpgsql IMMUTABLE
                       PARALLEL SAFE;


CREATE OR REPLACE FUNCTION revisions_change_notify() RETURNS trigger AS
$$
DECLARE
    revision   bigint;
    repository text;
    repo_id    integer;
BEGIN
    IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
        revision = NEW.revision;
        repo_id = NEW.repository;
    ELSE
        revision = OLD.revision;
        repo_id = OLD.repository;
    END IF;
    repository = (SELECT name FROM repositories WHERE id = repo_id);
    PERFORM pg_notify('revisions_change',
                      json_build_object('revision', revision, 'repository', repository, 'type', TG_OP)::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS revisions_change ON revisions;
CREATE TRIGGER revisions_change
    AFTER INSERT OR UPDATE OR DELETE
    ON revisions
    FOR EACH ROW
EXECUTE PROCEDURE revisions_change_notify();

CREATE MATERIALIZED VIEW IF NOT EXISTS revision_stats AS
WITH revs AS (SELECT revision, status FROM revisions)
SELECT revs.revision, count(objects.filename), sum(objects.len)
FROM revs
         LEFT JOIN objects ON revs.revision = objects.revision
WHERE revs.status = 'live'
GROUP BY revs.revision;

CREATE UNIQUE INDEX IF NOT EXISTS revision_stats_revision_idx ON revision_stats (revision);