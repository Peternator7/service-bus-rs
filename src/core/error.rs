use hyper;
use url;
use std::fmt::{Display, Formatter, Result};
use std::error::Error;
use std::convert::From;

#[derive(Debug)]
pub enum AzureRequestError {
    BadRequest, // StatusCode 400
    AuthorizationFailure, // StatusCode 401
    ResourceFailure, // StatusCode 403
    ResourceNotFound, // StatusCode 410
    InternalError, // StatusCode 500
    UnknownError, // Catch All
    InvalidEndpoint(url::ParseError), // Failure to parse URL
    HyperError(hyper::error::Error), // Hyper threw an error sending the request.
    LocalMessage, // The message doesn't exist on the server. You can't change it...
    EmptyBus, // There was nothing in the bus to receive.
    NonSerializedBody,
}

impl Error for AzureRequestError {
    fn description(&self) -> &str {
        use self::AzureRequestError::*;
        match self {
            &BadRequest => "Remote returned code 400.",
            &AuthorizationFailure => "Remote returned 401. Check your connection string.",
            &ResourceFailure => {
                "Message failed to send. The message may be too large or the queue is full."
            }
            &ResourceNotFound => "The requested queue does not exist or could not be found.",
            &InternalError => "Remote returned 500 - Internal server error",
            &UnknownError => "Something unexpected happened",
            &InvalidEndpoint(_) => "The provided URL could not be parsed",
            &HyperError(_) => "Hyper had an issue making a web request",
            &LocalMessage => {
                "The message doesn't exist on the server. This happens when you try and \
                 delete/lock a message you created locally."
            }
            &EmptyBus => {
                "Service Bus Queue/Subscription didn't have any messages before receive timed out."
            }
            &NonSerializedBody => {
                "Parsing the body failed. This happens if the message sender doesn't serialize the \
                 message. Call message.get_body_raw() to extract the body."
            }
        }
    }
}

impl Display for AzureRequestError {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "")
    }
}

impl From<url::ParseError> for AzureRequestError {
    fn from(err: url::ParseError) -> AzureRequestError {
        AzureRequestError::InvalidEndpoint(err)
    }
}

impl From<hyper::error::Error> for AzureRequestError {
    fn from(err: hyper::error::Error) -> AzureRequestError {
        AzureRequestError::HyperError(err)
    }
}