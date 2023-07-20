WITH ns AS (SELECT id FROM repositories WHERE name = $1)
SELECT revision, created_at
from revisions
WHERE repository IN (SELECT id FROM ns)
  AND status = 'live'
ORDER BY revision DESC
LIMIT 1;
