#![forbid(unsafe_code)]
// Logs go through `tracing`: a request must not write to stdout. The binary prints its commands.
#![cfg_attr(not(test), deny(clippy::print_stdout))]

pub mod config;
pub mod datamodel;
pub mod exporters;
pub mod http;
pub mod importers;
pub mod infer;
pub mod parsing;
pub mod storage;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
