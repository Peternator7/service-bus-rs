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

pub mod core;
pub mod servicebus;
