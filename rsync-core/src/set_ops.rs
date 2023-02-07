//! Operations on sorted async iterators.

use std::cmp::Ordering;
use std::future::Future;
use std::ops::ControlFlow;

use either::Either;
use eyre::Result;
use futures::{stream, Stream, TryStream, TryStreamExt};

/// Calculate union of two sorted async iterators.
pub fn into_union<I1, I2, F>(
    iter1: I1,
    iter2: I2,
    f_ord: F,
) -> impl Stream<Item = Result<Either<I1::Ok, I2::Ok>>>
where
    I1: TryStream<Error = eyre::Error> + Unpin + Send,
    I2: TryStream<Error = eyre::Error> + Unpin + Send,
    I1::Ok: Send + Sync,
    I2::Ok: Send + Sync,
    F: Fn(&I1::Ok, &I2::Ok) -> Ordering + Send + Sync,
{
    with_sorted_iter(iter1, iter2, f_ord, |x| async move {
        Ok(match x {
            Case::Left(i1) | Case::Both(i1, _) => ControlFlow::Break(Either::Left(i1)),
            Case::Right(i2) => ControlFlow::Break(Either::Right(i2)),
        })
    })
}

/// Calculate intersection of two sorted async iterators.
pub fn into_intersect<I1, I2, F>(
    iter1: I1,
    iter2: I2,
    f_ord: F,
) -> impl Stream<Item = Result<I1::Ok>>
where
    I1: TryStream<Error = eyre::Error> + Unpin + Send,
    I2: TryStream<Error = eyre::Error> + Unpin + Send,
    I1::Ok: Send + Sync,
    I2::Ok: Send + Sync,
    F: Fn(&I1::Ok, &I2::Ok) -> Ordering + Send + Sync,
{
    with_sorted_iter(iter1, iter2, f_ord, |x| async move {
        Ok(match x {
            Case::Left(_) | Case::Right(_) => ControlFlow::Continue(()),
            Case::Both(i1, _) => ControlFlow::Break(i1),
        })
    })
}

/// Calculate difference of two sorted async iterators.
pub fn into_diff<I1, I2, F>(iter1: I1, iter2: I2, f_ord: F) -> impl Stream<Item = Result<I1::Ok>>
where
    I1: TryStream<Error = eyre::Error> + Unpin + Send,
    I2: TryStream<Error = eyre::Error> + Unpin + Send,
    I1::Ok: Send + Sync,
    I2::Ok: Send + Sync,
    F: Fn(&I1::Ok, &I2::Ok) -> Ordering + Send + Sync,
{
    with_sorted_iter(iter1, iter2, f_ord, |x| async move {
        Ok(match x {
            Case::Left(i1) => ControlFlow::Break(i1),
            Case::Right(_) | Case::Both(_, _) => ControlFlow::Continue(()),
        })
    })
}

/// Calculate two way difference of two sorted async iterators.
pub fn into_two_way_diff<I1, I2, F>(
    iter1: I1,
    iter2: I2,
    f_ord: F,
) -> impl Stream<Item = Result<Either<I1::Ok, I2::Ok>>>
where
    I1: TryStream<Error = eyre::Error> + Unpin + Send,
    I2: TryStream<Error = eyre::Error> + Unpin + Send,
    I1::Ok: Send + Sync,
    I2::Ok: Send + Sync,
    F: Fn(&I1::Ok, &I2::Ok) -> Ordering + Send + Sync,
{
    with_sorted_iter(iter1, iter2, f_ord, |x| async move {
        Ok(match x {
            Case::Left(i1) => ControlFlow::Break(Either::Left(i1)),
            Case::Right(i2) => ControlFlow::Break(Either::Right(i2)),
            Case::Both(_, _) => ControlFlow::Continue(()),
        })
    })
}

pub fn with_sorted_iter<I1, I2, F, F2, Fut, U>(
    iter1: I1,
    iter2: I2,
    f_ord: F,
    f_case: F2,
) -> impl Stream<Item = Result<U>>
where
    I1: TryStream<Error = eyre::Error> + Unpin + Send,
    I2: TryStream<Error = eyre::Error> + Unpin + Send,
    I1::Ok: Send + Sync,
    I2::Ok: Send + Sync,
    F: Fn(&I1::Ok, &I2::Ok) -> Ordering + Send + Sync,
    F2: Fn(Case<I1::Ok, I2::Ok>) -> Fut + Send + Sync,
    Fut: Future<Output = Result<ControlFlow<U>>> + Send,
{
    stream::unfold(
        SortedIter {
            iter1,
            iter2,
            item1: None,
            item2: None,
            f_ord,
            f_case,
            initialized: false,
        },
        |mut it| async move { it.next_item().await.transpose().map(|i| (i, it)) },
    )
}

pub fn strip_either<I, T>(it: I) -> impl Stream<Item = Result<T>>
where
    I: TryStream<Ok = Either<T, T>, Error = eyre::Error> + Send,
    T: Send + Sync,
{
    it.try_filter_map(|i| async move {
        match i {
            Either::Left(i) | Either::Right(i) => Ok(Some(i)),
        }
    })
}

/// A helper to perform union/intersection/diff/two-way-diff on two sorted async iterators.
pub struct SortedIter<I1, I2, F, F2>
where
    I1: TryStream<Error = eyre::Error>,
    I2: TryStream<Error = eyre::Error>,
{
    iter1: I1,
    iter2: I2,
    item1: Option<I1::Ok>,
    item2: Option<I2::Ok>,
    f_ord: F,
    f_case: F2,
    initialized: bool,
}

pub enum Case<T1, T2> {
    Left(T1),
    Right(T2),
    Both(T1, T2),
}

macro_rules! apply_cf {
    ($e:expr) => {
        match $e {
            ControlFlow::Continue(()) => continue,
            ControlFlow::Break(x) => x,
        }
    };
}

impl<I1, I2, F, F2, Fut, U> SortedIter<I1, I2, F, F2>
where
    I1: TryStream<Error = eyre::Error> + Unpin + Send,
    I2: TryStream<Error = eyre::Error> + Unpin + Send,
    I1::Ok: Send + Sync,
    I2::Ok: Send + Sync,
    F: Fn(&I1::Ok, &I2::Ok) -> Ordering + Send + Sync,
    F2: Fn(Case<I1::Ok, I2::Ok>) -> Fut + Send + Sync,
    Fut: Future<Output = Result<ControlFlow<U>>> + Send,
{
    async fn next_item(&mut self) -> Result<Option<U>> {
        if !self.initialized {
            self.item1 = self.iter1.try_next().await?;
            self.item2 = self.iter2.try_next().await?;
            self.initialized = true;
        }

        loop {
            let item1 = self.item1.take();
            let item2 = self.item2.take();
            break match (item1, item2) {
                (Some(i1), Some(i2)) => match (self.f_ord)(&i1, &i2) {
                    Ordering::Less => {
                        self.item2 = Some(i2);
                        self.item1 = self.iter1.try_next().await?;
                        Ok(Some(apply_cf!((self.f_case)(Case::Left(i1)).await?)))
                    }
                    Ordering::Equal => {
                        self.item1 = self.iter1.try_next().await?;
                        self.item2 = self.iter2.try_next().await?;
                        Ok(Some(apply_cf!((self.f_case)(Case::Both(i1, i2)).await?)))
                    }
                    Ordering::Greater => {
                        self.item1 = Some(i1);
                        self.item2 = self.iter2.try_next().await?;
                        Ok(Some(apply_cf!((self.f_case)(Case::Right(i2)).await?)))
                    }
                },
                (Some(i1), None) => {
                    self.item1 = self.iter1.try_next().await?;
                    Ok(Some(apply_cf!((self.f_case)(Case::Left(i1)).await?)))
                }
                (None, Some(i2)) => {
                    self.item2 = self.iter2.try_next().await?;
                    Ok(Some(apply_cf!((self.f_case)(Case::Right(i2)).await?)))
                }
                (None, None) => Ok(None),
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use either::Either;
    use futures::{stream, StreamExt, TryStreamExt};

    use crate::set_ops::{into_diff, into_intersect, into_two_way_diff, into_union, strip_either};

    #[tokio::test]
    async fn must_strip_either() {
        let it = stream::iter([Either::Left(1), Either::Right(2), Either::Left(3)]).map(Ok);
        assert_eq!(
            strip_either(it).try_collect::<Vec<_>>().await.unwrap(),
            vec![1, 2, 3]
        );
    }

    #[tokio::test]
    async fn must_union() {
        let it_1 = stream::iter([1, 4, 5, 6, 8]).map(Ok);
        let it_2 = stream::iter([2, 3, 4, 5, 7, 8, 9]).map(Ok);
        assert_eq!(
            into_union(it_1, it_2, Ord::cmp)
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            vec![
                Either::Left(1),
                Either::Right(2),
                Either::Right(3),
                Either::Left(4),
                Either::Left(5),
                Either::Left(6),
                Either::Right(7),
                Either::Left(8),
                Either::Right(9),
            ]
        );
    }

    #[tokio::test]
    async fn must_intersect() {
        let it_1 = stream::iter([1, 4, 5, 6, 8]).map(Ok);
        let it_2 = stream::iter([2, 3, 4, 5, 7, 8, 9]).map(Ok);
        assert_eq!(
            into_intersect(it_1, it_2, Ord::cmp)
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            vec![4, 5, 8]
        );
    }

    #[tokio::test]
    async fn must_diff() {
        let it_1 = stream::iter([1, 4, 5, 6, 8]).map(Ok);
        let it_2 = stream::iter([2, 3, 4, 5, 7, 8, 9]).map(Ok);
        assert_eq!(
            into_diff(it_1, it_2, Ord::cmp)
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            vec![1, 6]
        );
    }

    #[tokio::test]
    async fn must_two_way_diff() {
        let it_1 = stream::iter([1, 4, 5, 6, 8]).map(Ok);
        let it_2 = stream::iter([2, 3, 4, 5, 7, 8, 9]).map(Ok);
        assert_eq!(
            into_two_way_diff(it_1, it_2, Ord::cmp)
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            vec![
                Either::Left(1),
                Either::Right(2),
                Either::Right(3),
                Either::Left(6),
                Either::Right(7),
                Either::Right(9),
            ]
        );
    }
}
