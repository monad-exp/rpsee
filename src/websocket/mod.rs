//! # `websocket` module
//!
//! The most complex part of Rpsee, the WS module contains all
//! the necessary functions to open connections between Rpsee and clients,
//! Rpsee and RPC nodes, and manage subscriptions.
//!
//! For increased efficency, reliability and performance, clients are never directly
//! routed to nodes. Instead, all connections go through Rpsee, and then are passed
//! through selected healthy RPC endpoints. This allows Rpsee to cache requests and
//! responses as usual, and always have an open connection with a client, even in case
//! some nodes become unhealthy.
//!
//! Subscriptions are handled in much the same way. Rpsee will only subscribe to one
//! subscription once. In case multiple clients subscribe to the same subscription,
//! Rpsee will deduplicate responses to them from a single subscription it has made.
//!
//! All of this happens so that user don't need to take any actions in case of node failiures.

pub mod client;
pub mod error;
pub mod server;
pub mod subscription_manager;
pub mod types;
