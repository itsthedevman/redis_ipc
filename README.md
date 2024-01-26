# Ruby IPC using Redis Streams and Consumer Groups

## Overview

The `redis_ipc` gem provides a simple way to implement [Inter-Process Communication (IPC)](https://en.wikipedia.org/wiki/Inter-process_communication) between n-number of Ruby processes via [Redis Streams](https://redis.io/docs/data-types/streams/).

## What is RedisIPC?

- Simple and easy to implement
- Quick and efficient communication between n-number of programs/processes
- Thread-safe, process-safe, and load-balanced
- **Inherently insecure**
  - Any process that has access to the Redis database can add/change/remove data in transit.
  - **Do not use RedisIPC in an untrusted environment!**
- Extensible
  - Within Ruby, `RedisIPC::Stream` can be adapted to different use cases. See `RedisIPC::Channel`
  - Outside of Ruby, any programming language with access to Redis can implement the Redis Stream and Consumer Group design and hook into RedisIPC allowing for communication between completely different programming languages.
  - With a centralized Redis instance, programs on separate computers can communicate with each other

---

## Installation

Add this line to your application's Gemfile:

```ruby
gem "redis_ipc"
```

And then execute:

```bash
$ bundle
```

Or install it yourself as:

```bash
$ gem install redis_ipc
```

## `RedisIPC::Stream` usage

```ruby
require "redis_ipc"

# The "stream_name" is the shared stream for all endpoints
# The "group_name" is the unique identifier for this endpoint.
# Processes cannot share the same group at this current point
stream = RedisIPC::Stream.new("stream_name", "group_name")

# Any entries sent to our group will come through here
stream.on_request do |entry|
  # Do something with entry (RedisIPC::Stream::Entry)
end

# Any exceptions that occurred will come through here
stream.on_error do |exception|
  # Do something with exception (StandardError, or similar)
end

# Until this point, this endpoint was not connected to the stream.
# Calling #connect will allow us to send and receive entries on the stream
#
# All of the following options are pre-configured so they do not have to be provided
stream.connect(
  # Redis connection options. Host, port, url, etc.
  redis_options: {},

  # An optional logger instance that can be provided for verbose logs
  # Default: nil
  logger: nil,

  # The number of Redis connections to be allotted for sending entries. This number is added
  # To the total number of Redis connections based on consumer/dispatcher pool sizes.
  # Redis connections are not held onto for very long so this number does not need to be huge
  # Default: 10
  pool_size: 10,

  # The max pool size for Redis connections for this stream/group.
  # When provided, this option will take precedence over `pool_size` and the related
  # calculations mentioned above.
  # Default: nil
  max_pool_size: nil,

  # Options for the ledger
  ledger: {
    # Controls how long, in seconds, an entry can be in the ledger before it is considered expired
    # This also controls how long the thread is blocked when waiting for an entry to be responded to
    entry_timeout: 5,

    # Controls how often, in seconds, the ledger should clean up and remove expired entries
    cleanup_interval: 1
  },

  # Options for the consumers
  consumer: {
    # How many consumers should be created
    pool_size: 3,

    # How often, in seconds, should consumers check for entries
    execution_interval: 0.001
  },

  # Options for the dispatchers
  dispatcher: {
    # How many dispatchers should be created
    pool_size: 2,

    # How often, in seconds, should dispatchers check for entries
    execution_interval: 0.001
  }
)

# Now we can send data
# This will block until "another_group" picks up the entry, or until the timeout is reached, in which this will raise an exception
stream.send_to_group(content: "Hello!", to: "another_group")
```

## `RedisIPC::Stream` example

Run the following code in a Ruby IRB process.
_If running this code from a script, make sure the script does not exit unless told to._

```ruby
require "redis_ipc"

# Setup the basic stream
child_stream = RedisIPC::Stream.new("electric_household", "child")
child_stream.on_error do |exception|
  puts exception
end

# When an entry comes in, have a 75% chance of fulfilling the request, and a 25% chance of being a baby
child_stream.on_request do |entry|
  if rand > 0.25
    fulfill_request(entry, content: "Yes, creators")
  else
    reject_request(entry, content: "No! <Tantrum initiated>")
  end
end

# Connect to the stream
child_stream.connect
```

And then run this code in another Ruby IRB process, separate from the one above.
_Although, it can work in the same process._

```ruby
require "redis_ipc"

parent_stream = RedisIPC::Stream.new("electric_household", "parent")
parent_stream.on_error do |exception|
  puts exception
end

# This parent isn't very nice
parent_stream.on_request do |entry|
  reject_request(entry, content: "No, and because I said so")
end

# Connect to the stream
parent_stream.connect

# Ask the child for something
response = parent_stream.send_to_group(content: "Please do this task for me, child", to: "child")

# Handle the child's reaction
if response.fulfilled?
  puts "Child said: #{response.value}"
  exit 0
end

# The child is throwing a tantrum
response = parent_stream.send_to_group(content: "Child, I will not ask again", to: "child")
if response.rejected? # Why yes, this is totally reasonable parenting (sarcasm)
  puts "The child's reasoning is #{response.reason}"
  exit 1
end
```

## `RedisIPC::Channel`

`RedisIPC::Channel` is an event based implementation of Stream. It still uses `RedisIPC::Stream` underneath but instead of accepting any data that is sent to the group it only allows certain events with predefined data structures. This is incredibly useful for programs that have specific functionality they want to expose to other programs. This can also be thought of like an API.

### `RedisIPC::Channel` example

Let's say you have a website and one worker that will need to occasionally trigger each others logic. Since they both have access to Redis, this can be solved using `Channel`.

In the website codebase, you would add a new class that inherits from `RedisIPC::Channel` and configure it:

```rb
class WebChannel < RedisIPC::Channel
  # Sets the name of the stream.
  # Channels must be on the same stream in order to trigger each others events
  stream "ipc:website"

  # The unique identifier for this class.
  # This name is used by other channels to trigger events defined below
  channel "web"

  # An event are similar to an API endpoint. Other channels can trigger these events and receive
  # The params argument is an allowlist that can contain Strings/Symbols (either work)
  # Only the keys defined in params argument array will be available in the params hash below
  event "notifications::create", params: [:title, :message] do
    # The params hash is HashWithIndifferentAccess
    # The result of this block is sent back to the channel that triggered this event
    Notification.create!(**params)
  end

  # This event will return `true` back to the channel that triggered this event
  event "notifications::delete", params: [:id] do
    Notification.delete(params[:id])

    true
  end
end

# Called somewhere during program lifecycle
WebChannel.connect
```

The worker will also need a new class created in its own codebase:

```rb
class WorkerChannel < RedisIPC::Channel
  # Notice this is the same as WebChannel above
  stream "ipc:website"

  # Since Channel is just an implementation of Stream, .group can also be used instead of .channel
  channel "worker"

  event("start_job", params: [:id]) { JobSite.start_job(params["id"]) }

  # Events do not have to have params
  event("active_jobs_report") { Foreman.queue_report("active_jobs").to_h }
end

# Called somewhere during program lifecycle
# .connect is a wrapper for RedisIPC::Stream#connect allowing you to configure the underlying functionality
WorkerChannel.connect(redis_options: {url: ENV["REDIS_URL"]})
```

Now that both program channels are connected to the same stream, they can now communicate with each other via `.trigger_event`.

```rb
# On the website, let's start a job
WebChannel.trigger_event("start_job", target: "worker", params: {id: 1})

# And run a report
response = WebChannel.trigger_event("active_jobs_report", target: "worker")
if response.fulfilled?
  puts response.value #=> {worker_1: [{id: 1, status: :running}]}
end
```

### Channels (and Streams) can support n-number of endpoints

Now that the website and worker are communicating, you find yourself needing a tracker program to trigger web notifications when it starts to track an event. Since Streams can support n-number of endpoints, so can Channels. Let's add a new class to the tracker program:

```rb
class TrackerChannel < RedisIPC::Channel
  stream "ipc:website"
  channel "tracker"

  # Since .connect is a class method, it can be called within the class itself
  # This is also showing off adjusting an underlying configuration option in the ledger
  connect(ledger: {entry_timeout: 10})
end

# We can now trigger web notifications
response = TrackerChannel.trigger_event(
  "notifications::create",
  params: {title: "Tracker event", message: "A new event has been tracked"},
  target: "web"
)
response.status #=> :fulfilled
```

### Exceptions in events

Whenever an exception is raised, or an entry is manually rejected like in a Stream (see Timeouts below), the response will be in a rejected state and the reason for the rejection can be accessed using `#reason`.
```rb
class Buggy < RedisIPC::Channel
  stream "example"
  channel "buggy"

  event(:raise_exception) { raise "I told you!" }
  connect
end

class Endpoint < RedisIPC::Channel
  stream "example"
  channel "endpoint"
  connect
end

response = Endpoint.trigger_event(:raise_exception, target: "buggy")
response.rejected? #=> true
response.reason #=> "I told you!"
```

## Timeouts

When a Stream, or Channel, hits the timeout on an outbound request the resulting response will be rejected with a TimeoutError.

```rb

stream = RedisIPC::Stream.new("example", "group")
stream.on_request {}
stream.on_error {}
stream.connect

# This will block until `entry_timeout` is hit. By default, this will take 5 seconds
response = stream.send_to_group(content: "This message will never get there", to: "a_group_that_does_not_exist")
response.state #=> :rejected
response.reason #=> #<RedisIPC::TimeoutError: RedisIPC::TimeoutError>
```

## Contributing

Contributions are welcome! RedisIPC has a great foundation and has everything I need it to have, but it is far from feature complete. If you find any issues or have suggestions for improvement, please open an issue or create a pull request.

## Development

1. Clone the repository:

   ```bash
   git clone https://github.com/itsthedevman/redis_ipc.git
   ```

2. Install dependencies:

   ```bash
   bundle install
   ```

3. Run the tests:

   ```bash
   bundle exec rspec spec
   ```

### What is a Stream?

At the core of RedisIPC is the Stream (`RedisIPC::Stream`). The Stream class is the abstracted implementation of the underlying Consumer Group functionality. It provides ways to configure, connect, send, and receive data. The way Stream does this is by hosting n-number of Consumers and n-number of Dispatchers, along with a Ledger.

### What is a Consumer?

A Consumer (`RedisIPC::Stream::Consumer`) is an Ruby implementation of a Redis Consumer and its default job is to watch the stream and read in entries and process them. The Stream, however, uses a special Consumer called Ledger Consumer to handle processing entries instead for reasons which will be explained soon.

### What is a Dispatcher?

A Dispatcher (`RedisIPC::Stream::Dispatcher`) is another special Consumer that is essentially a bouncer for the Stream. Every entry posted to the Redis Stream is received by every Dispatcher, regardless of the group it belongs to and where the entry is going. Once a Dispatcher receives an entry from the stream for its group and dispatch to group consumer, depending on the state of the entry and which instance it originated from.

### What is the Ledger and Ledger Consumer?

The Ledger (`RedisIPC::Stream::Ledger`) is the way the Stream tracks and handles timeouts for outbound entries. This is where Ledger Consumer (`RedisIPC::Stream::Ledger::Consumer`) come in. When entries are dispatched to them, they will check to see if the entry is in the Ledger. If the entry exists, the Ledger Consumer will then pass the entry to the waiting code. Entries that do not exist in the ledger are treated as inbound requests which bubble up to the Stream itself. This functionality, plus statuses, ensure entries are processed correctly without confusion.

### Process-safe?
Yes! As of 1.0.2, RedisIPC can be safely used in programs that have multiple processes, or instances, of them running at once.
This is managed via an instance ID that is unique to each instance of a Stream/Channel which allows entries to be properly dispatched to the instance that original made the request. With this change, all Dispatchers across the instances for a group now work together to dispatch entries to each others consumers.

## License

The gem is available as open source under the terms of the [MIT License](https://github.com/itsthedevman/redis_ipc/blob/main/LICENSE).
