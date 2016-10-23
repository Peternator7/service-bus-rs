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

const CONTENT_TYPE: &'static str = "application/atom+xml;type=entry;charset=utf-8";
const SAS_BUFFER_TIME: usize = 15;

// Ideally this should be a field on the client, but we don't want to expose it through
// the trait, and if we don't then we can't share code.
lazy_static!{
    static ref CLIENT: Client = Client::new();
}

/// The Queue Trait is an abstraction over different types of Queues that
/// can be used when communicating with an Azure Service Bus Queue. The concurrent version
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
pub trait Subscription
    where Self: Sized
{
    /// The subscription name.
    fn subscription(&self) -> &str;

    /// The topic name.
    fn topic(&self) -> &str;

    /// Regenerates the SAS string if it is close to expiring. Returns a valid SAS string
    /// This function may return the same String multiple times.
    fn refresh_sas(&self) -> String;

    /// The endpoint for the Queue. `http://{namespace}.servicebus.net/`
    fn endpoint(&self) -> &url::Url;


    /// Receive a message from the queue. Returns either the deserialized message or an error
    /// detailing what went wrong. The message will not be deleted on the server until
    /// `queue_client.complete_message(message)` is called. This is ideal for applications that
    /// can't afford to miss a message.
    fn receive(&self) -> Result<BrokeredMessage, AzureRequestError> {
        let timeout = Duration::from_secs(30);
        self.receive_with_timeout(timeout)
    }

    /// Receive a message from the queue. Returns the deserialized message or an error.
    /// The message is deleted from the queue when it is received. If the application crashes,
    /// the contents of the message can be lost.
    fn receive_and_delete(&self) -> Result<BrokeredMessage, AzureRequestError> {
        let timeout = Duration::from_secs(30);
        self.receive_and_delete_with_timeout(timeout)
    }

    /// Receive a message from the queue. Returns the deserialized message or an error.
    /// The message is deleted from the queue when it is received. If the application crashes,
    /// the contents of the message can be lost.
    fn receive_and_delete_with_timeout(&self,
                                       timeout: Duration)
                                       -> Result<BrokeredMessage, AzureRequestError> {

        let sas = self.refresh_sas();
        let path = format!("{}/subscriptions/{}/messages/head?timeout={}",
                           self.topic(),
                           self.subscription(),
                           timeout.as_secs());

        let uri = self.endpoint().join(&path)?;
        let mut header = Headers::new();
        header.set(Authorization(sas));

        let response = CLIENT.delete(uri).headers(header).send()?;
        interpret_results(response.status)?;
        Ok(BrokeredMessage::with_response(response))
    }

    /// Receive a message from the queue. Returns either the deserialized message or an error
    /// detailing what went wrong. The message will not be deleted on the server until
    /// `queue_client.complete_message(message)` is called. This is ideal for applications that
    /// can't afford to miss a message. Allows a timeout to be specified for greater control.
    fn receive_with_timeout(&self,
                            timeout: Duration)
                            -> Result<BrokeredMessage, AzureRequestError> {
        let sas = self.refresh_sas();
        let path = format!("{}/subscriptions/{}/messages/head?timeout={}",
                           self.topic(),
                           self.subscription(),
                           timeout.as_secs());

        let uri = self.endpoint().join(&path)?;

        let mut header = Headers::new();
        header.set(Authorization(sas));
        let response = CLIENT.post(uri).headers(header).send()?;
        interpret_results(response.status)?;
        Ok(BrokeredMessage::with_response(response))
    }

    /// Completes a message that has been received from the Service Bus. This will fail
    /// if the message was created locally. Once a message is created, it cannot be restored
    ///
    /// ```
    /// let message = my_queue.receive().unwrap();
    /// // Do lots of processing with the message. Send it to another database.
    /// my_queue.complete_message(message);
    /// ```
    fn complete_message(&self, message: BrokeredMessage) -> Result<(), AzureRequestError> {
        let sas = self.refresh_sas();

        // Take either the Sequence number or the Message ID
        // Then add the lock token and finally join it into the targer
        let target = get_message_update_path(self, &message)?;

        let mut header = Headers::new();
        header.set(Authorization(sas));
        let response = CLIENT.delete(target).headers(header).send()?;
        interpret_results(response.status)

    }

    /// Releases the lock on a message and puts it back into the queue.
    /// This method generally indicates that the message could not be
    /// handled properly and should be attempted at a later time.
    fn abandon_message(&self, message: BrokeredMessage) -> Result<(), AzureRequestError> {
        let sas = self.refresh_sas();

        // Take either the Sequence number or the Message ID
        // Then add the lock token and finally join it into the targer
        let target = get_message_update_path(self, &message)?;

        let mut header = Headers::new();
        header.set(Authorization(sas));
        let response = CLIENT.put(target).headers(header).send()?;
        interpret_results(response.status)

    }

    /// Renews the lock on a message. If a message is received by calling
    /// `queue.receive()` or `queue.receive_with_timeout()` then the message is locked
    /// but not deleted on the Service Bus. This method allows the lock to be renewed
    /// if additional time is needed to finish processing the message.
    ///
    /// ```
    /// use std::thread::sleep;
    ///
    /// let message = queue.receive();
    /// sleep(2*60*1000);
    /// //Renew the lock on the message so that we can keep processing it.
    /// queue.renew_message(message);
    /// sleep(2*60*1000);
    /// queue.complete_message(message);
    /// ```
    fn renew_message(&self, message: &BrokeredMessage) -> Result<(), AzureRequestError> {
        let sas = self.refresh_sas();

        // Take either the Sequence number or the Message ID
        // Then add the lock token and finally join it into the targer
        let target = get_message_update_path(self, &message)?;

        let mut header = Headers::new();
        header.set(Authorization(sas));
        let response = CLIENT.post(target).headers(header).send()?;
        interpret_results(response.status)
    }

    /// Creates an event loop for handling messages that blocks the current thread.
    ///
    /// ```
    /// queue.on_message(|message| {
    ///     // Do message processing
    ///     queue.complete_message(message);
    /// });
    /// ```
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

/// Client for sending and receiving messages from a Service Bus Queue in Azure.
/// This cient is `!Sync` because it internally uses a RefCell to keep track of
/// its authorization token, but it is still ideal for single threaded use.
pub struct SubscriptionClient {
    connection_string: String,
    topic_name: String,
    subscription_name: String,
    endpoint: url::Url,
    sas_info: RefCell<(String, usize)>,
}

impl SubscriptionClient {
    /// Create a new queue with a connection string and the name of a queue.
    /// The connection string can be copied and pasted from the azure portal.
    /// The queue name should be the name of an existing queue.
    pub fn with_conn_and_queue(connection_string: &str,
                               topic: &str,
                               subscription: &str)
                               -> Result<SubscriptionClient, url::ParseError> {
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

                Ok(SubscriptionClient {
                    connection_string: conn_string,
                    subscription_name: subscription.to_string(),
                    topic_name: topic.to_string(),
                    endpoint: url,
                    sas_info: RefCell::new((sas_key, expiry - SAS_BUFFER_TIME)),
                })
            }
            Err(e) => Err(e),
        }
    }
}

impl Subscription for SubscriptionClient {
    fn subscription(&self) -> &str {
        &self.subscription_name
    }

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

/// The ConcurrentQueueClient has all the same methods as QueueClient, but it is also
/// `Sync`. This means that it can be shared between threads. Prefer using a Arc<ConcurrentQueueClient>
/// over an Arc<Mutex<QueueClient>> to share the thread between queues.
///
/// ```
/// use std::thread;
/// let queue = Arc::new(ConcurrentQueueClient::with_conn_and_queue(conn,queue_name));
/// for _ in 0..10 {
///     let q = queue.clone();
///     thread::spawn(move || {
///         q.send(BrokeredMessage::with_body("Sending a concurrent message"));
///     });
/// }
/// ```
pub struct ConcurrentSubscriptionClient {
    connection_string: String,
    topic_name: String,
    subscription_name: String,
    endpoint: url::Url,
    sas_info: Mutex<(String, usize)>,
}

impl ConcurrentSubscriptionClient {
    pub fn with_conn_and_queue(connection_string: &str,
                               topic: &str,
                               subscription: &str)
                               -> Result<ConcurrentSubscriptionClient, url::ParseError> {
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

                Ok(ConcurrentSubscriptionClient {
                    connection_string: conn_string,
                    subscription_name: subscription.to_string(),
                    topic_name: topic.to_string(),
                    endpoint: url,
                    sas_info: Mutex::new((sas_key, expiry - SAS_BUFFER_TIME)),
                })
            }
            Err(e) => Err(e),
        }
    }
}

impl Subscription for ConcurrentSubscriptionClient {
    fn subscription(&self) -> &str {
        &self.subscription_name
    }

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

// Complete, Abandon, Renew all make calls to the same Uri so here's a quick function
// for generating it.
fn get_message_update_path<T>(q: &T,
                              message: &BrokeredMessage)
                              -> Result<url::Url, AzureRequestError>
    where T: Subscription
{

    // Take either the Sequence number or the Message ID
    // Then add the lock token and finally join it into the targer
    let target = message.props
        .SequenceNumber
        .map(|seq| seq.to_string())
        .or(message.props.MessageId.clone())
        .and_then(|id| message.props.LockToken.as_ref().map(|lock| (id, lock)))
        .map(|(id, lock)| {
            format!("{}/subscriptions/{}/messages/{}/{}",
                    q.topic(),
                    q.subscription(),
                    id,
                    lock)
        })
        .and_then(|path| q.endpoint().join(&*path).ok())
        .ok_or(AzureRequestError::LocalMessage);
    target
}