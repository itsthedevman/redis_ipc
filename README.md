# Inter-process communication via Redis Streams for Ruby

## Overview

The `redis_ipc` gem provides a simple way to implement Inter-Process Communication (IPC) between 2 or more Ruby processes by utilizing Redis Streams as a message broker. Since the entire communication workflow happens via Redis, this design also supports communicating with other processes that can utilize Redis Streams so long as the data structure is the same.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'redis_ipc'
```

And then execute:

```bash
$ bundle
```

Or install it yourself as:

```bash
$ gem install redis_ipc
```

## Basic Usage

```ruby
require "redis_ipc"

# The "stream_name" is the shared stream for all endpoints
# The "group_name" is the unique identifier for this endpoint. Processes cannot share the same group at this current point
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
stream.connect

# Now we can send data
# This will block until "another_group" picks up the entry, or until the timeout is reached, in which this will raise an exception
stream.send_to_group(content: "Hello!", to: "another_group")
```

## Realistic example

Run this code in one Ruby process:

```ruby
require "redis_ipc"

# Setup the basic stream
child_stream = RedisIPC::Stream.new("electric_household", "child")
child_stream.on_error do |exception|
  puts exception # This method would log the exception and do other business logic, if it existed
end

# When an entry comes in, have a 75% chance of fulfilling the request, and a 25% chance of being a baby
child_stream.on_request do |entry|
  if rand > 0.25
    fulfill_request(entry, content: "Yes, creators")
  else
    reject_request(entry, content: "No! <Tantrum initiated>")
  end
end

# All of the following options are pre-configured so they do not have to be provided
child_stream.connect(
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
    execution_interval: 0.01
  },

  # Options for the dispatchers
  dispatcher: {
    # How many dispatchers should be created
    pool_size: 2,

    # How often, in seconds, should dispatchers check for entries
    execution_interval: 0.01
  }
)
```

And then run this code in another Ruby process, separate from the one above. _Although, it can work in the same process_

```ruby
require "redis_ipc"

parent_stream = RedisIPC::Stream.new("electric_household", "parent")
parent_stream.on_error do |exception|
  log_exception(exception) # This method would log the exception and do other business logic, if it existed
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
  puts "Child said: #{response.content}"
  exit 0
end

# The child is throwing a tantrum
response = parent_stream.send_to_group(content: "Child, I will not ask again", to: "child")
exit 1 if response.rejected? # Why yes, this is totally reasonable parenting (sarcasm)
```

## Contributing

Contributions are welcome! If you find any issues or have suggestions for improvement, please open an issue or create a pull request.

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

## License

The gem is available as open source under the terms of the [MIT License](https://github.com/itsthedevman/redis_ipc/blob/main/LICENSE).
