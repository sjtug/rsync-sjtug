use std::fmt::{Debug, Display, Formatter};

use actix_web::http::StatusCode;
use actix_web::ResponseError;
use eyre::Report;
use tokio::task::JoinHandle;

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

/// Wrapper around `tokio::task::JoinHandle` that aborts the task when dropped.
pub struct AbortJoinHandle<T>(JoinHandle<T>);

impl<T> AbortJoinHandle<T> {
    pub fn new(handle: JoinHandle<T>) -> Self {
        Self(handle)
    }
}

impl<T> Drop for AbortJoinHandle<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
