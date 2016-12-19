#![feature(proc_macro)]

extern crate crypto;
#[macro_use]
extern crate hyper;
#[macro_use]
extern crate lazy_static;
extern crate rustc_serialize as serialize;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate time as time2;
#[macro_use]
extern crate url;

#[macro_use]
mod macros;

/// Contains shared functionality between all the different
/// modules inside of the Azure Libary
///
pub mod core;

/// The Service Bus module provides a wrapper around the Azure Service Bus
/// REST Api's. This includes the Service Queue and Topics.
/// They communicate messages through the BrokeredMessage struct.
///
pub mod servicebus;
