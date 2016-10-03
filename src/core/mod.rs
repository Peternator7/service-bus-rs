pub mod error;

use std::time;

use time2;
use crypto::mac::Mac;
use crypto::hmac::Hmac;
use crypto::sha2::*;

use serialize::base64::{self, ToBase64};
use url::percent_encoding::{utf8_percent_encode, USERINFO_ENCODE_SET, SIMPLE_ENCODE_SET};

/// This function generates a sas token for authenticating into azure
/// using the connection string provided on portal.azure.com.
/// It will not raise an error if there is a mistake in the connection string,
/// but the token will be invalid.
pub fn generate_sas(connection_string: &str, duration: time::Duration) -> (String, usize) {
    define_encode_set! {
        pub CUSTOM_ENCODE_SET = [SIMPLE_ENCODE_SET] | {'+', '=', '/'}
    }

    let mut key = "";
    let mut endpoint = "";
    let mut name = "";

    let params = connection_string.split(";");
    for param in params {
        let idx = param.find("=").unwrap_or(0);
        let (mut k, mut value) = param.split_at(idx);
        k = k.trim();
        value = value.trim();
        // cut out the equal sign if there was one.
        if value.len() > 0 {
            value = &value[1..]
        }
        match k {
            "Endpoint" => endpoint = value,
            "SharedAccessKey" => key = value,
            "SharedAccessKeyName" => name = value,
            _ => {}
        };
    }

    let mut h = Hmac::new(Sha256::new(), key.as_bytes());

    let time2_duration = time2::Duration::from_std(duration).unwrap_or(time2::Duration::seconds(0));
    let encoded_url = utf8_percent_encode(endpoint, USERINFO_ENCODE_SET).collect::<String>();
    let expiry = (time2::now_utc() + time2_duration).to_timespec().sec;

    let message = format!("{}\n{}", encoded_url, expiry);
    h.input(message.as_bytes());

    let mut sig = h.result().code().to_base64(base64::STANDARD);
    sig = utf8_percent_encode(&sig, CUSTOM_ENCODE_SET).collect::<String>();

    let sas = format!("SharedAccessSignature sig={}&se={}&skn={}&sr={}",
                      sig,
                      expiry,
                      name,
                      encoded_url);

    (sas, expiry as usize)
}