use std::time::Duration;
use std::cell::RefCell;
use std::sync::Mutex;

use core::generate_sas;
use core::error::AzureRequestError;
use super::brokeredmessage::*;

use url;
use time2;
use hyper::client::Client;
use hyper::header::*;
use hyper::status::StatusCode;
use hyper::mime::Mime;

const CONTENT_TYPE: &'static str = "application/atom+xml;type=entry;charset=utf-8";
const SAS_BUFFER_TIME: usize = 15;

// Ideally this should be a field on the client, but we don't want to expose it through
// the trait, and if we don't then we can't share code.
lazy_static!{
    static ref CLIENT: Client = Client::new();
}

pub trait Queue {
    fn queue(&self) -> &str;
    fn refresh_sas(&self) -> String;
    fn endpoint(&self) -> &url::Url;

    fn send(&self, message: BrokeredMessage) -> Result<(), AzureRequestError> {
        let timeout = Duration::from_secs(30);
        self.send_with_timeout(message, timeout)
    }

    fn receive(&self) -> Result<BrokeredMessage, AzureRequestError> {
        let timeout = Duration::from_secs(30);
        self.receive_with_timeout(timeout)
    }

    fn receive_and_delete(&self) -> Result<BrokeredMessage, AzureRequestError> {
        let timeout = Duration::from_secs(30);
        self.receive_and_delete_with_timeout(timeout)
    }

    fn send_with_timeout(&self,
                         message: BrokeredMessage,
                         timeout: Duration)
                         -> Result<(), AzureRequestError> {

        let sas = self.refresh_sas();
        let path = format!("{}/messages?timeout={}", self.queue(), timeout.as_secs());
        match self.endpoint().join(&*path) {
            Ok(uri) => {
                // let sas = self.sas_key.borrow().clone();
                let mut header = Headers::new();
                header.set(Authorization(sas));

                // This will always succeed.
                let content_type: Mime = CONTENT_TYPE.parse().unwrap();
                header.set(ContentType(content_type));
                header.set(BrokerPropertiesHeader(message.props_as_json()));

                let result = CLIENT.post(uri).headers(header).body(message.get_body_raw()).send();

                match result {
                    Ok(response) => interpret_results(response.status),
                    Err(_) => Err(AzureRequestError::HyperError),
                }
            }
            Err(_) => Err(AzureRequestError::InvalidEndpoint),
        }
    }

    fn receive_and_delete_with_timeout(&self,
                                       timeout: Duration)
                                       -> Result<BrokeredMessage, AzureRequestError> {

        let sas = self.refresh_sas();
        let path = format!("{}/messages/head?timeout={}",
                           self.queue(),
                           timeout.as_secs());

        match self.endpoint().join(&*path) {
            Ok(uri) => {
                let mut header = Headers::new();
                header.set(Authorization(sas));
                let result = CLIENT.delete(uri).headers(header).send();
                if let Ok(response) = result {
                    match interpret_results(response.status) {
                        Ok(()) => Ok(BrokeredMessage::with_response(response)),
                        Err(e) => Err(e),
                    }
                } else {
                    Err(AzureRequestError::HyperError)
                }
            }
            Err(_) => Err(AzureRequestError::InvalidEndpoint),
        }
    }
    fn receive_with_timeout(&self,
                            timeout: Duration)
                            -> Result<BrokeredMessage, AzureRequestError> {
        let sas = self.refresh_sas();
        let path = format!("{}/messages/head?timeout={}",
                           self.queue(),
                           timeout.as_secs());

        match self.endpoint().join(&*path) {
            Ok(uri) => {
                let mut header = Headers::new();
                header.set(Authorization(sas));
                let result = CLIENT.post(uri).headers(header).send();
                if let Ok(response) = result {
                    match interpret_results(response.status) {
                        Ok(()) => Ok(BrokeredMessage::with_response(response)),
                        Err(e) => Err(e),
                    }
                } else {
                    Err(AzureRequestError::HyperError)
                }
            }
            Err(_) => Err(AzureRequestError::InvalidEndpoint),
        }
    }

    fn complete_message(&self, message: BrokeredMessage) -> Result<(), AzureRequestError> {
        let sas = self.refresh_sas();

        // Take either the Sequence number or the Message ID
        // Then add the lock token and finally join it into the targer
        let target = message.props
            .SequenceNumber
            .map(|seq| seq.to_string())
            .or(message.props.MessageId.clone())
            .and_then(|id| message.props.LockToken.as_ref().map(|lock| (id, lock)))
            .map(|(id, lock)| format!("/{}/messages/{}/{}", self.queue(), id, lock))
            .and_then(|path| self.endpoint().join(&*path).ok());

        if let Some(target) = target {
            let mut header = Headers::new();
            header.set(Authorization(sas));
            let result = CLIENT.delete(target).headers(header).send();
            if let Ok(response) = result {
                interpret_results(response.status)
            } else {
                Err(AzureRequestError::HyperError)
            }
        } else {
            Err(AzureRequestError::LocalMessage)
        }
    }

    fn on_message<H>(&self, handler: H) -> AzureRequestError
        where H: Fn(BrokeredMessage)
    {
        loop {
            let res = self.receive_and_delete();
            match res {
                Ok(message) => handler(message),
                Err(AzureRequestError::EmptyBus) => {}
                Err(e) => return e,
            }
        }
    }
}

// This version is Send but not Sync
// It's probably slightly more performant than the concurrent version.
pub struct QueueClient {
    connection_string: String,
    queue_name: String,
    endpoint: url::Url,
    sas_info: RefCell<(String, usize)>,
}

impl QueueClient {
    pub fn with_conn_and_queue(connection_string: &str,
                               queue: &str)
                               -> Result<QueueClient, url::ParseError> {
        let duration = Duration::from_secs(60 * 6);
        let mut endpoint = String::new();
        for param in connection_string.split(";") {
            let idx = param.find("=").unwrap_or(0);
            let (mut k, mut value) = param.split_at(idx);
            k = k.trim();
            value = value.trim();
            // cut out the equal sign if there was one.
            if value.len() > 0 {
                value = &value[1..]
            }
            match k {
                "Endpoint" => endpoint = value.to_string(),
                _ => {}
            };
        }
        endpoint = String::new() + "https" + endpoint.split_at(endpoint.find(":").unwrap_or(0)).1;
        match url::Url::parse(&*endpoint) {
            Ok(url) => {
                let (sas_key, expiry) = generate_sas(connection_string, duration);
                let conn_string = connection_string.to_string();

                Ok(QueueClient {
                    connection_string: conn_string,
                    queue_name: queue.to_string(),
                    endpoint: url,
                    sas_info: RefCell::new((sas_key, expiry - SAS_BUFFER_TIME)),
                })
            }
            Err(e) => Err(e),
        }
    }
}

impl Queue for QueueClient {
    fn queue(&self) -> &str {
        &self.queue_name
    }

    fn endpoint(&self) -> &url::Url {
        &self.endpoint
    }

    fn refresh_sas(&self) -> String {
        let curr_time = time2::now_utc().to_timespec().sec as usize;
        let mut sas_tuple = self.sas_info.borrow_mut();
        if curr_time > sas_tuple.1 {
            let duration = Duration::from_secs(60 * 6);
            let (key, expiry) = generate_sas(&*self.connection_string, duration);
            sas_tuple.1 = expiry;
            sas_tuple.0 = key;
        }
        sas_tuple.0.clone()
    }
}

// The ConcurrentQueueClient is Send and Sync
// Wrap it in an Arc and it can be shared between threads freely.
pub struct ConcurrentQueueClient {
    connection_string: String,
    queue_name: String,
    endpoint: url::Url,
    sas_info: Mutex<(String, usize)>,
}

impl ConcurrentQueueClient {
    pub fn with_conn_and_queue(connection_string: &str,
                               queue: &str)
                               -> Result<ConcurrentQueueClient, url::ParseError> {
        let duration = Duration::from_secs(60 * 6);
        let mut endpoint = String::new();
        for param in connection_string.split(";") {
            let idx = param.find("=").unwrap_or(0);
            let (mut k, mut value) = param.split_at(idx);
            k = k.trim();
            value = value.trim();
            // cut out the equal sign if there was one.
            if value.len() > 0 {
                value = &value[1..]
            }
            match k {
                "Endpoint" => endpoint = value.to_string(),
                _ => {}
            };
        }
        endpoint = String::new() + "https" + endpoint.split_at(endpoint.find(":").unwrap_or(0)).1;
        match url::Url::parse(&*endpoint) {
            Ok(url) => {
                let (sas_key, expiry) = generate_sas(connection_string, duration);
                let conn_string = connection_string.to_string();

                Ok(ConcurrentQueueClient {
                    connection_string: conn_string,
                    queue_name: queue.to_string(),
                    endpoint: url,
                    sas_info: Mutex::new((sas_key, expiry - SAS_BUFFER_TIME)),
                })
            }
            Err(e) => Err(e),
        }
    }
}

impl Queue for ConcurrentQueueClient {
    fn queue(&self) -> &str {
        &self.queue_name
    }

    fn endpoint(&self) -> &url::Url {
        &self.endpoint
    }

    fn refresh_sas(&self) -> String {
        let curr_time = time2::now_utc().to_timespec().sec as usize;
        let mut sas_tuple = match self.sas_info.lock() {
            Ok(guard) => guard,
            Err(poison) => poison.into_inner(),
        };
        if curr_time > sas_tuple.1 {
            let duration = Duration::from_secs(60 * 6);
            let (key, expiry) = generate_sas(&*self.connection_string, duration);
            sas_tuple.1 = expiry;
            sas_tuple.0 = key;
        }
        sas_tuple.0.clone()
    }
}


// Here's one function that interprets what all of the error codes mean for consistency.
// This might even get elevated out of this module, but preferabbly not.
fn interpret_results(status: StatusCode) -> Result<(), AzureRequestError> {
    use core::error::AzureRequestError::*;
    match status {
        StatusCode::Unauthorized => Err(AuthorizationFailure),
        StatusCode::InternalServerError => Err(InternalError),
        StatusCode::BadRequest => Err(BadRequest),
        StatusCode::Forbidden => Err(ResourceFailure),
        StatusCode::Gone => Err(ResourceNotFound),
        // These are the successful cases.
        StatusCode::Created => Ok(()),
        StatusCode::Ok => Ok(()),
        _ => Err(UnknownError),
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use servicebus::brokeredmessage::BrokeredMessage;

    lazy_static!{
        static ref CONNECTION_STRING: String = {
            use std::io::BufReader;
            use std::fs::File;
            use std::io::prelude::*;
            let mut s = String::new();
            let mut reader = BufReader::new(File::open("connection_string.txt").unwrap());
            reader.read_line(&mut s).unwrap();
            s
        };
    }

    #[test]
    fn queue_send_message() {
        let queue = QueueClient::with_conn_and_queue(&CONNECTION_STRING, "test1").unwrap();
        let message = BrokeredMessage::with_body("Cats and Dogs");
        match queue.send(message) {
            Err(e) => {
                println!("{:?}", e);
                panic!("Failed to send message.")
            }
            _ => assert!(true),
        }
    }

    #[test]
    fn queue_receive_message() {
        let queue = QueueClient::with_conn_and_queue(&*CONNECTION_STRING, "test1").unwrap();
        match queue.receive_and_delete() {
            Err(e) => {
                println!("{:?}", e);
                panic!("Failed to receive message.")
            }
            Ok(_) => {}
        }
    }

    #[test]
    fn queue_complete_message() {
        let queue = QueueClient::with_conn_and_queue(&*CONNECTION_STRING, "test1").unwrap();
        queue.send(BrokeredMessage::with_body("Complete this message")).unwrap();
        match queue.receive() {
            Err(e) => {
                println!("{:?}", e);
                panic!("Failed to receive message.")
            }
            Ok(message) => {
                match queue.complete_message(message.clone()) {
                    Err(e) => {
                        println!("{:?}", e);
                        println!("{:?}", message);
                        panic!("Failed to complete the message");
                    }
                    _ => {}
                }
            }
        }
    }

    #[test]
    fn conncurrent_queue_send_message() {
        let queue = ConcurrentQueueClient::with_conn_and_queue(&*CONNECTION_STRING, "test1")
            .unwrap();
        let message = BrokeredMessage::with_body("Cats and Dogs");
        match queue.send(message) {
            Err(e) => {
                println!("{:?}", e);
                panic!("Failed to send message.");
            }
            _ => {}
        }
    }

    #[test]
    fn concurrent_queue_receive_message() {
        let queue = ConcurrentQueueClient::with_conn_and_queue(&*CONNECTION_STRING, "test1")
            .unwrap();
        match queue.receive_and_delete() {
            Err(e) => {
                println!("{:?}", e);
                panic!("Failed to receive a message");
            }
            Ok(_) => {}
        }
    }
}