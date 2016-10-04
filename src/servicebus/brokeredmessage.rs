use std::io::Read;
use core::error::AzureRequestError;
use hyper::client::Response;
use serde_json;

header! { (BrokerPropertiesHeader, "BrokerProperties") => [String] }

// We need to extract all the properties that can be passed to the object.
#[allow(non_snake_case)]
#[derive(Eq,PartialEq,Clone,Debug,Serialize,Deserialize,Default)]
pub struct BrokerProperties {
    // These 3 fields let you complete/abandon a request.
    #[serde(skip_serializing_if="Option::is_none")]
    pub LockToken: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub MessageId: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub SequenceNumber: Option<usize>,

    #[serde(skip_serializing_if="Option::is_none")]
    pub DeliveryCount: Option<usize>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub EnqueuedSequenceNumber: Option<usize>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub EnqueuedTimeUtc: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub Label: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub State: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub TimeToLive: Option<usize>,
}

#[derive(Clone,Eq,PartialEq,Debug)]
pub struct BrokeredMessage {
    pub props: Box<BrokerProperties>,
    body: String,
}

impl BrokeredMessage {
    pub fn with_body(body: &str) -> BrokeredMessage {
        BrokeredMessage {
            body: format!("<string>{}</string>", body),
            props: Box::new(Default::default()),
        }
    }

    pub fn with_body_and_props(body: &str, props: BrokerProperties) -> BrokeredMessage {
        BrokeredMessage {
            body: body.to_string(),
            props: Box::new(props),
        }
    }

    pub fn with_response(mut response: Response) -> BrokeredMessage {
        let mut body = String::new();
        response.read_to_string(&mut body).ok();

        let props = response.headers
            .get::<BrokerPropertiesHeader>()
            .and_then(|header| serde_json::from_str::<BrokerProperties>(header).ok())
            .unwrap_or(Default::default());

        BrokeredMessage {
            body: body,
            props: Box::new(props),
        }
    }

    /// Attempts to deserialize the body into a String.
    /// This can fail. We do this here so that if a message is
    /// incorrectly formatted it can still be used.
    pub fn get_body(&self) -> Result<String, AzureRequestError> {
        // Get the opening of the first string.
        self.body.trim()
            .find("<string")
            .and_then(|idx| if idx == 0 {Some(self.body.split_at(idx).1) } else { None })
            // Look for the closing brace and take everything in the middle.
            .and_then(|rhs| rhs.rfind("</string>").map(|idx| rhs.split_at(idx).0))
            // Skip the rest of the opening tag. Then build a new string from the rest of the body.
            .map(|inner| inner.chars().skip_while(|&ch| ch != '>').skip(1).collect::<String>())
            // If any of these steps failed, return an error.
            .ok_or(AzureRequestError::NonSerializedBody)
    }

    pub fn get_body_raw(&self) -> &str {
        &self.body
    }

    pub fn props_as_json(&self) -> String {
        serde_json::to_string(&self.props).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_empty_test() {
        let message = BrokeredMessage::with_body("");
        assert_eq!(Ok(String::from("")), message.get_body());
        assert_eq!("<string></string>", message.get_body_raw());
    }

    #[test]
    fn message_deserializes_test() {
        let message = BrokeredMessage::with_body("<b>Hello World</b>");
        assert_eq!(Ok(String::from("<b>Hello World</b>")), message.get_body());
        assert_eq!("<string><b>Hello World</b></string>",
                   message.get_body_raw());
    }

    #[test]
    fn message_json_test() {
        let message = BrokeredMessage::with_body("{\"Azure\":2}");
        assert_eq!(Ok(String::from("{\"Azure\":2}")), message.get_body());
        assert_eq!("<string>{\"Azure\":2}</string>", message.get_body_raw());
    }
}
