//! Newline-delimited JSON-RPC: framing, message types, and the request
//! handlers shared by the stdio, persistent-daemon, and WebSocket transports.

pub mod frame;
pub mod server;
pub mod types;

pub use frame::{FrameWriter, MAX_RPC_FRAME_BYTES, RpcFrame, StdioFrameWriter, read_rpc_frame};
pub use server::RpcServer;
pub use types::{
    RpcError, RpcEvent, RpcRequest, RpcResponse, get_bool_param, get_int_param, get_string_param,
};
