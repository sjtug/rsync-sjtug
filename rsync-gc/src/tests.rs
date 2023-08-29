mod db_required {
    use std::collections::BTreeMap;

    use futures::TryStreamExt;
    use sqlx::types::chrono::Utc;
    use sqlx::{Acquire, PgPool, Postgres};

    use crate::pg::mark_stale;
    use rsync_core::pg::{
        change_revision_status, create_revision, ensure_repository, RevisionStatus,
    };
    use rsync_core::tests::generate_random_namespace;

    async fn create_rev_live<'a>(
        namespace: &'a str,
        db: impl Acquire<'a, Database = Postgres>,
    ) -> i32 {
        let mut conn = db.acquire().await.unwrap();
        let rev = create_revision(namespace, RevisionStatus::Partial, &mut *conn)
            .await
            .unwrap();
        change_revision_status(rev, RevisionStatus::Live, Some(Utc::now()), &mut *conn)
            .await
            .unwrap();
        rev
    }

    async fn create_by_plan<'a>(
        plan: &'a [RevisionStatus],
        namespace: &'a str,
        db: impl Acquire<'a, Database = Postgres>,
    ) -> Vec<i32> {
        let mut conn = db.acquire().await.unwrap();
        let mut revs = vec![];
        for status in plan {
            let rev = if *status == RevisionStatus::Live {
                create_rev_live(namespace, &mut *conn).await
            } else {
                create_revision(namespace, *status, &mut *conn)
                    .await
                    .unwrap()
            };
            revs.push(rev);
        }

        revs
    }

    struct RevAndStatus {
        revision: i32,
        status: RevisionStatus,
    }

    async fn assert_revs(
        revs: &[i32],
        expected: &[RevisionStatus],
        namespace: &str,
        pool: &PgPool,
    ) {
        assert_eq!(
            revs.len(),
            expected.len(),
            "revs and expected must have the same length"
        );
        let mut actual_revs: BTreeMap<_, _> = sqlx::query_as!(
            RevAndStatus,
            r#"
SELECT revision, status as "status: _" from revisions
WHERE repository in (SELECT id FROM repositories WHERE name = $1)
    "#,
            namespace
        )
        .map(|r| (r.revision, r.status))
        .fetch(pool)
        .try_collect()
        .await
        .unwrap();

        let revs = revs.to_vec();
        for (idx, rev) in revs.iter().enumerate() {
            let status = actual_revs.remove(rev);
            assert_eq!(
                status,
                Some(expected[idx]),
                "revision {rev}({idx}) has unexpected status: expected {:?}, actual {status:?}",
                Some(expected[idx]),
            );
        }

        assert!(
            actual_revs.is_empty(),
            "unexpected revisions: {actual_revs:?}"
        );
    }

    #[sqlx::test(migrations = "../tests/migrations")]
    async fn must_mark_stale(pool: PgPool) {
        use RevisionStatus::*;

        let mut conn = pool.acquire().await.unwrap();

        for (before, live, partial, expected) in [
            // 1. must remove all partial indices before the last live index
            (
                &[Partial, Live, Partial, Live, Stale, Live, Partial][..],
                usize::MAX,
                usize::MAX,
                &[Stale, Live, Stale, Live, Stale, Live, Partial][..],
            ),
            // 2. must remove all partial indices after the last live index other than the last `keep_partial` partial indices
            (
                &[Partial, Live, Partial, Partial, Partial][..],
                usize::MAX,
                2,
                &[Stale, Live, Stale, Partial, Partial][..],
            ),
            // 3. must remove all live indices other than the last `keep_live` live indices
            (
                &[Live, Live, Stale, Live, Partial][..],
                2,
                usize::MAX,
                &[Stale, Live, Stale, Live, Partial][..],
            ),
            // 4. must run 3 after 2
            (
                &[Live, Live, Stale, Live, Partial][..],
                0,
                usize::MAX,
                &[Stale, Stale, Stale, Stale, Partial][..],
            ),
            // 5. must not remove partial when there's no live index
            (
                &[Partial, Stale, Partial, Partial][..],
                usize::MAX,
                usize::MAX,
                &[Partial, Stale, Partial, Partial][..],
            ),
        ] {
            let namespace = generate_random_namespace();
            ensure_repository(&namespace, &mut conn).await.unwrap();
            let revs = create_by_plan(before, &namespace, &pool).await;
            mark_stale(&namespace, live, partial, &pool).await.unwrap();
            assert_revs(&revs, expected, &namespace, &pool).await;
        }
    }
}
