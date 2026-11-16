#![forbid(unsafe_code)]
#![warn(clippy::nursery)]
//! Veyrmux: safe, Tokio-native, Yamux v0-compatible stream multiplexing.
//!
//! The connection must be continuously driven while its streams are in use.
//! Frames reach the underlying writer with header and payload contiguous.
//! Stream flow control and independent half-close are preserved.
mod frame;
mod session;
mod stream;

#[cfg(feature = "futures-compat")]
pub mod yamux_shim;

pub use session::{Config, Connection, ConnectionError, Mode, Role, StreamId};
pub use stream::Stream;
