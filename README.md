## ServiceBus Rust

This is a library designed to allow calls to the Azure Service Bus Rest API through the [Rust] (https://www.rust-lang.org/en-US/) 
programming language.

### What is Rust?

Rust is a systems programming language designed to prevent segfaults and guarentee thread-safety. This makes it an excellent choice
for many types of applications.

### What is an Azure Service Bus?

Azure is a cloud platform operated by Microsoft. A Service Bus is a resource designed for passing messages between applications.
Service Bus's contain different types of objects for receiving and sending messages. The simplest type is the Queue. A queue is a 
FIFO structure that receives messages and stores them until they are removed for processing. Messages can be put back in the queue
if they can't be processed or added to a Dead-Letter Queue meaning that they were unprocessable.

The other model is to use a combination of Topics and Subscriptions. Messages are sent to a topic just like they are sent to a queue.
To retrieve a message, it must be read out of a subscription. Every subscriptions are attached to a topic and each 
subscription receives its own copy of every message that comes into the topic. This makes it simple for different processes 
to all receive messages without having to share them. This might be useful in a situation where one process handles messages 
and other process receives messages for logging and auditting.


```

          |----- Subscription 1
          |
Topic-----|----- Subscription 2
          |
          |----- Subscription 3
          
```

The different ServiceBus models allow for load-balancing, asyncronous processing, and message relaying. Queues and Subscriptions
make it easy to perform load balancing. Different processes can listen on the same queue and work will be distributed between all of them.
Queues store their messages until they are requested which makes it easy to have a client send a message without having to wait for a 
server to respond. ServiceBus's can also be used to relay messages from Azure to On-Premise solutions.

### How to use this library

This library can be used on the server, but it's probably most useful running in client applications. This library isn't hosted on Cargo
yet so to use it, import the package directly from this repository

```rust
extern crate servicebus_rs;

fn main() {
}
```

If you want to create a client for any of the types use one of these:

```rust
fn main() {
  let queue = QueueClient::create(connection_string, queue_name);
  let topic = TopicClient::create(connection_string, topic_name);
  let subscription = SubscriptionClient::create(connection_string, topic_name, subscription_name);
}
```

Each of the clients have a concurrent version `ConcurrentQueueClient`... that can be passed across threads. None of the clients
have asyncronous processing yet so this enables them to be easily shared between threads.

```rust
// Example of concurrent message processing in this library.
fn main() {
  let queue = Arc::new(ConcurrentQueueClient::create(connection_string, queue_name));
  for _ in 0..10 {
    thread::spawn(|| {
      let message = queue.receive();
      // Do message processing.
    });
  }
}
```

Prefer using the concurrent versions to Arc<Mutex<Client>>. Only a small part of the message sending and receiving requires
syncronization so the concurrent versions leverage that fact for increased parallelism.

## The Message Body

To allow for better interoperability with the .Net libraries, this library attempts to deserialize the message body when receiving
it and automatically wraps the messages it sends in <string></string> tags to allow for deserialization in .Net libaries.

The Rust library currently only supports serializing/deserializing strings. This may change in the future, but in the meantime,
serialize your object before adding it to the BrokeredMessage.

## Disclaimer

I work for Microsoft Corporation, but this project is not endorsed or sponsored by Microsoft and entirely uses resources that are
publicly available on the internet.
