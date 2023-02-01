use std::ffi::OsString;

#[derive(Debug, Clone)]
pub enum Rule {
    Exclude(OsString),
    Include(OsString),
}
