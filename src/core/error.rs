#[derive(Eq,PartialEq,Debug)]
pub enum AzureRequestError {
    BadRequest, // StatusCode 400
    AuthorizationFailure, // StatusCode 401
    ResourceFailure, // StatusCode 403
    ResourceNotFound, // StatusCode 410
    InternalError, // StatusCode 500
    UnknownError, // Catch All
    InvalidEndpoint, // Failure to parse URL
    HyperError, // Hyper threw an error sending the request.
    LocalMessage, // The message doesn't exist on the server. You can't change it...
    EmptyBus, // There was nothing in the bus to receive.
    NonSerializedBody,
}