#![feature(conservative_impl_trait)]
extern crate time as time2;
extern crate crypto;
extern crate rustc_serialize as serialize;
#[macro_use]
extern crate url;
#[macro_use]
extern crate lazy_static;
extern crate hyper;

// use crypto::mac::Mac;
// use crypto::hmac::Hmac;
// use crypto::sha2::*;
//
// use serialize::base64::{self, ToBase64};
//
// use url::percent_encoding::{utf8_percent_encode, USERINFO_ENCODE_SET};
//
const CONNECTION_STRING: &'static str = "Endpoint=sb://personal.servicebus.windows.net/;\
                                         SharedAccessKeyName=RootManageSharedAccessKey;\
                                         SharedAccessKey=/ltI/wU/bT4cqc+V/C5KDwxG8rjHqU5IOS+aC2j4aQo=";
// const PRIMARY_KEY: &'static str = "/ltI/wU/bT4cqc+V/C5KDwxG8rjHqU5IOS+aC2j4aQo=";
// const QUEUE_URL: &'static str = "sb://personal.servicebus.windows.net/";
//

mod core;
mod servicebus;

pub fn main() {
    // let mut h = Hmac::new(Sha256::new(), PRIMARY_KEY.as_bytes());
    // let expiry = time2::now_utc() + time2::Duration::minutes(600);
    // println!("{}\n", expiry.to_timespec().sec);
    //
    // let encoded_url = utf8_percent_encode(QUEUE_URL, USERINFO_ENCODE_SET).collect::<String>();
    // println!("{:?}\n", encoded_url);
    //
    // let message = format!("{}\n{}", encoded_url, expiry.to_timespec().sec);
    // println!("{}\n", message);
    //
    // h.input(message.as_bytes());
    // let mut sig = h.result().code().to_base64(base64::STANDARD);
    // sig = utf8_percent_encode(&sig, USERINFO_ENCODE_SET).collect::<String>();
    // println!("{:?}\n", sig);
    //
    // let sas = format!("SharedAccessSignature sig={}&se={}&skn={}&sr={}",
    // sig,
    // expiry.to_timespec().sec,
    // "RootManageSharedAccessKey",
    // encoded_url);
    //


    let sas2 = core::generate_sas(CONNECTION_STRING, std::time::Duration::from_secs(600));
    println!("\n\n");
    println!("{}", sas2);
}