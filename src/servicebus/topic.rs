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

/// The Topic Trait is an abstraction over different types of Topics that
/// can be used when communicating with an Azure Service Bus Topic. The concurrent version
/// is both send and sync. All methods take a reference to the client so the ConcurrentQueueClient
/// can be used across multiple threads by simply wrapping it in an Arc<T>. The only step that is
/// synchronized is regenerating the auth key so prefer this struct to using an Arc<Mutex<Queue>>
///
/// The non-concurrent version isn't Sync because it internally uses a RefCell to hide the fact
/// that it will generate new credentials occasionally. Prefer this version if you don't need
/// synchronization as it avoids the overhead of obtaining a mutex lock.
///
/// Topics and subscriptions work together hand in hand. Together they provide similar functionality
/// to queues. Producers send message to the topic. Consumers then create a subscription to the
/// topic to receive every messages. Each subscription functions as an individual queue. This is useful
/// when the same message will be read multiple times for different reasons. An example might be
/// adding logging to the Service bus. One subscription might be used to provide load balancing for
/// servers as described in the Queue page. Another subscription will log every message as they come in
/// in a different subscription. This way different processes can consume the message and not interfere
/// with each other or have to worry about losing messages.
pub trait Topic
    where Self: Sized
{
    /// The topic name.
    fn topic(&self) -> &str;

    /// Regenerates the SAS string if it is close to expiring. Returns a valid SAS string
    /// This function may return the same String multiple times.
    fn refresh_sas(&self) -> String;

    /// The endpoint for the topic. `http://{namespace}.servicebus.net/`
    fn endpoint(&self) -> &url::Url;

    /// Send a message to the topic. Consumes the message. If the server returned an error
    /// Then this function will return an error. The default timeout is 30 seconds.
    ///
    /// ```
    /// use servicebus::brokeredmessage::BrokeredMessage;
    ///
    /// let message = BrokeredMessage::with_body("This is a message");
    /// match my_queue.send(message) {
    ///     Ok(_) => println!("The message sent successfully"),
    ///     Err(e) => println!("The error was: {:?}", e);
    /// }
    /// ```
    fn send(&self, message: BrokeredMessage) -> Result<(), AzureRequestError> {
        let timeout = Duration::from_secs(30);
        self.send_with_timeout(message, timeout)
    }


    /// Sends a message to the Service Bus Topic with a designated timeout.
    fn send_with_timeout(&self,
                         message: BrokeredMessage,
                         timeout: Duration)
                         -> Result<(), AzureRequestError> {

        let sas = self.refresh_sas();
        let path = format!("{}/messages?timeout={}", self.topic(), timeout.as_secs());
        let uri = self.endpoint().join(&path)?;

        let mut header = Headers::new();
        header.set(Authorization(sas));

        // This will always succeed.
        let content_type: Mime = CONTENT_TYPE.parse().unwrap();
        header.set(ContentType(content_type));
        header.set(BrokerPropertiesHeader(message.props_as_json()));

        let response = CLIENT.post(uri).headers(header).body(&message.serialize_body()).send()?;
        interpret_results(response.status)
    }
}

/// Client for sending messages to a Service Bus Topic in Azure.
/// This cient is `!Sync` because it internally uses a RefCell to keep track of
/// its authorization token, but it is still ideal for single threaded use.
pub struct TopicClient {
    connection_string: String,
    topic_name: String,
    endpoint: url::Url,
    sas_info: RefCell<(String, usize)>,
}

impl TopicClient {
    /// Create a new queue with a connection string and the name of a topic.
    /// The connection string can be copied and pasted from the azure portal.
    /// The queue name should be the name of an existing topic.
    pub fn with_conn_and_topic(connection_string: &str,
                               topic: &str)
                               -> Result<TopicClient, url::ParseError> {
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
        let url = url::Url::parse(&endpoint)?;
        let (sas_key, expiry) = generate_sas(connection_string, duration);
        let conn_string = connection_string.to_string();

        Ok(TopicClient {
            connection_string: conn_string,
            topic_name: topic.to_string(),
            endpoint: url,
            sas_info: RefCell::new((sas_key, expiry - SAS_BUFFER_TIME)),
        })
    }
}

impl Topic for TopicClient {
    fn topic(&self) -> &str {
        &self.topic_name
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

/// The ConcurrentTopicClient has all the same methods as TopicClient, but it is also
/// `Sync`. This means that it can be shared between threads. Prefer using a Arc<ConcurrentTopicClient>
/// over an Arc<Mutex<TopicClient>> to share the thread between queues.
///
/// ```
/// use std::thread;
/// let topic = Arc::new(ConcurrentTopicClient::with_conn_and_topic(conn,topic_name));
/// for _ in 0..10 {
///     let t = topic.clone();
///     thread::spawn(move || {
///         t.send(BrokeredMessage::with_body("Sending a concurrent message"));
///     });
/// }
/// ```
pub struct ConcurrentTopicClient {
    connection_string: String,
    topic_name: String,
    endpoint: url::Url,
    sas_info: Mutex<(String, usize)>,
}

impl ConcurrentTopicClient {
    pub fn with_conn_and_topic(connection_string: &str,
                               topic: &str)
                               -> Result<ConcurrentTopicClient, url::ParseError> {
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
        let url = url::Url::parse(&endpoint)?;
        let (sas_key, expiry) = generate_sas(connection_string, duration);
        let conn_string = connection_string.to_string();

        Ok(ConcurrentTopicClient {
            connection_string: conn_string,
            topic_name: topic.to_string(),
            endpoint: url,
            sas_info: Mutex::new((sas_key, expiry - SAS_BUFFER_TIME)),
        })
    }
}

impl Topic for ConcurrentTopicClient {
    fn topic(&self) -> &str {
        &self.topic_name
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
