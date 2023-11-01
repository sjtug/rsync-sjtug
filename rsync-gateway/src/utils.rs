use std::fmt::{Debug, Display, Formatter};

use actix_web::http::StatusCode;
use actix_web::ResponseError;
use eyre::Report;
use rkyv::with::{ArchiveWith, DeserializeWith, SerializeWith};
use rkyv::Fallible;

/// Wrapper around `eyre::Report` that implements `actix_web::ResponseError`.
pub struct ReportWrapper {
    report: Report,
    status: StatusCode,
}

impl Debug for ReportWrapper {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        <Report as Debug>::fmt(&self.report, f)
    }
}

impl Display for ReportWrapper {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        <Report as Display>::fmt(&self.report, f)
    }
}

impl ResponseError for ReportWrapper {
    fn status_code(&self) -> StatusCode {
        self.status
    }
}

pub trait ReportExt {
    type Output;
    /// Convert `Report` into `ReportWrapper` with status code 500.
    fn into_resp_err(self) -> Self::Output;
    /// Convert `Report` into `ReportWrapper` with the given status code.
    fn with_status(self, status: StatusCode) -> Self::Output;
}

impl ReportExt for Report {
    type Output = ReportWrapper;
    fn into_resp_err(self) -> Self::Output {
        ReportWrapper {
            report: self,
            status: StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn with_status(self, status: StatusCode) -> Self::Output {
        ReportWrapper {
            report: self,
            status,
        }
    }
}

impl<T> ReportExt for Result<T, Report> {
    type Output = Result<T, ReportWrapper>;
    fn into_resp_err(self) -> Self::Output {
        self.map_err(ReportExt::into_resp_err)
    }

    fn with_status(self, status: StatusCode) -> Self::Output {
        self.map_err(|e| e.with_status(status))
    }
}

pub struct SkipRkyv;

impl<T> ArchiveWith<T> for SkipRkyv {
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve_with(_: &T, _: usize, (): Self::Resolver, _: *mut Self::Archived) {
        panic!("Attempted to resolve a field explicitly skipped")
    }
}

impl<T, S> SerializeWith<T, S> for SkipRkyv
where
    S: Fallible,
{
    fn serialize_with(_: &T, _: &mut S) -> Result<Self::Resolver, S::Error> {
        panic!("Attempted to serialize a field explicitly skipped")
    }
}

impl<F, T, D> DeserializeWith<F, T, D> for SkipRkyv
where
    D: Fallible,
{
    fn deserialize_with(_: &F, _: &mut D) -> Result<T, D::Error> {
        panic!("Attempted to deserialize a field explicitly skipped")
    }
}
