#![feature(plugin,custom_derive)]
#![plugin(serde_macros)]
#![allow(dead_code)]
extern crate time as time2;
extern crate crypto;
extern crate rustc_serialize as serialize;
#[macro_use]
extern crate url;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate hyper;
extern crate serde;
extern crate serde_json;

/// Contains shared functionality between all the different
/// modules inside of the Azure Libary
///
pub mod core;

/// The Service Bus module provides a wrapper around the Azure Service Bus
/// REST Api's. This includes the Service Queue and Topics.
/// They communicate messages through the BrokeredMessage struct.
///
pub mod servicebus;
