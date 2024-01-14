# frozen_string_literal: true

module RedisIPC
  class Stream
    class_attribute :stream_name
    class_attribute :group_name

    #
    # Joins a Stream with the given name and creates the group with the given name
    #
    # @param stream [String] The name of the Stream
    # @param group [String] The name of the group to connect as. This must be unique!
    #
    def initialize(stream, group)
      self.stream_name = stream.to_s
      self.group_name = group.to_s

      @on_request = nil
      @on_error = nil
    end

    #
    # Sets the on_request callback that is called when the group receives an inbound request entry
    #
    # @param &block [Proc] This callback is provided a single argument which is the inbound request entry
    #
    def on_request(&block)
      @on_request = block
      self
    end

    #
    # Sets the on_error callback that is called if some part of the process raises an exception
    #
    # @param &block [Proc] This callback is provided a single argument which is the exception
    #
    def on_error(&block)
      @on_error = block
      self
    end

    #
    # Creates the consumers/dispatchers and connects to the stream
    #
    # @param redis_options [Hash] The connection options passed into Redis. See redis_rb
    # @param **options [Hash] Configuration options for Stream, Consumer, Dispatcher, and Ledger
    #
    # @option options [Logger, NilClass] :logger
    #   An optional logger instance to enable logging
    #   Default: nil
    #
    # @option options [Integer] :pool_size
    #   The size of the pool of Redis clients to make available for sending.
    #   The stream will automatically configure the required number of Redis clients for reading.
    #   Ignored if max_pool_size is set
    #   Default: 10
    #
    # @option options [Integer, NilClass] :max_pool_size
    #   When provided, this force sets the Redis client pool size to be max_pool_size.
    #   This disables the automatic configuration mentioned above
    #   Default: nil
    #
    # @option options [Hash] :ledger
    #   Configuration options for the ledger
    # @option ledger [Integer] :entry_timeout
    #   How long should it take before the entry is considered timed-out when sending requests
    #   Default: 5 (seconds)
    # @option ledger [Integer] :cleanup_interval
    #   How often should the ledger check for expired entries before removing them
    #   Default: 1 (second)
    #
    # @option options [Hash] :consumer
    #   Configuration options for the Consumers
    # @option consumer [Integer] :pool_size
    #   How many Consumers should be created for processing entries. These will be load balanced by the Dispatchers
    #   Default: 10
    # @option consumer [Integer] :execution_interval
    #   How often should the consumer check for entries to be processed
    #   Default: 0.001 (seconds. This is 1ms)
    #
    # @option options [Hash] :dispatcher
    # @option consumer [Integer] :pool_size
    #   How many Dispatchers should be created for processing entries
    #   Default: 3
    # @option consumer [Integer] :execution_interval
    #   How often should the Dispatcher check for entries to be dispatched
    #   Default: 0.001 (seconds. This is 1ms)
    #
    # @return [RedisIPC::Stream] self
    #
    def connect(redis_options: {}, **options)
      check_for_valid_configuration!

      options = {
        logger: nil,
        pool_size: 10,
        max_pool_size: nil,
        ledger: Ledger::DEFAULTS,
        consumer: Ledger::Consumer::DEFAULTS,
        dispatcher: Dispatcher::DEFAULTS
      }.deep_merge(options)

      #
      # The redis pool is shared between the stream, all consumers, and all dispatchers.
      #
      # A consumer (Consumer, Dispatcher, Ledger::Consumer) should only ever use one Redis client at a time
      # since there are no blocking commands and tasks cannot stack. This means the bulk of the pool size
      # is dependent on how many threads are going to be sending entries at a single time.
      #
      # Since this code has no real life testing, this default is purely based on educated guesses.
      # To make sure the connection pool is sufficiently padded, I'm allotting two connections per consumer which
      # makes the default max pool size of 36 (=10 + (10 * 2) + (3 * 2))
      #
      max_pool_size = options[:max_pool_size] || (
        options[:pool_size] +
        (options.dig(:consumer, :pool_size) * 2) +
        (options.dig(:dispatcher, :pool_size) * 2)
      )

      @logger = options[:logger]
      @redis = Commands.new(
        stream_name, group_name,
        max_pool_size: max_pool_size,
        redis_options: redis_options,
        **options.slice(:logger, :reset)
      )

      # Recreate the group
      @redis.destroy_group
      @redis.create_group

      @ledger = Ledger.new(options[:ledger])
      @consumers = create_consumers(options[:consumer])
      @dispatchers = create_dispatchers(options[:dispatcher])

      log("Connected")
      self
    end

    #
    # Is the Stream connected or not?
    #
    # @return [Boolean]
    #
    def connected?
      !@consumers.nil? && !@dispatchers.nil?
    end

    #
    # Disconnects the Stream from Redis and stops processing entries
    #
    def disconnect
      # #connect might've not been called...
      @dispatchers&.each(&:stop_listening)
      @consumers&.each(&:stop_listening)
      @redis&.shutdown

      @dispatchers = nil
      @consumers = nil
      @ledger = nil
      @redis = nil

      log("Disconnected")

      self
    end

    #
    # Sends data (content) to a group on the Stream
    #
    # @param content [Object] The data to send. Must be JSON compatible
    # @param to [String] The group to send the content to
    #
    # @return [RedisIPC::Response] The result of the data
    #   Use #fulfilled? to check if the request was a success
    #   If fulfilled, use #value to get the value
    #   Use #rejected? to check if the request was rejected or raised an exception
    #   If rejected, use #reason to get the reason. This could be anything, but likely a String or an error instance
    #
    def send_to_group(content:, to:)
      check_for_ledger!

      track_and_send(content, to)
    end

    #
    # Used when responding to a request, this marks the entry as fulfilled and sends it back to the sending group
    #
    # @param entry [RedisIPC::Stream::Entry] The request entry to respond to
    # @param content [Object] The data to send back
    #
    def fulfill_request(entry, content:)
      check_for_ledger!

      log("Fulfilling entry #{entry.id} from \"#{entry.destination_group}\" with: #{content}")

      @redis.add_to_stream(entry.fulfilled(content: content))

      nil
    end

    #
    # Used when responding to a request, this marks the entry as rejected and sends it back to the sending group
    #
    # @param entry [RedisIPC::Stream::Entry] The request entry to respond to
    # @param content [Object] The data to send back
    #
    def reject_request(entry, content:)
      check_for_ledger!

      log("Rejecting entry #{entry.id} from \"#{entry.destination_group}\" with: #{content}")

      @redis.add_to_stream(entry.rejected(content: content))

      nil
    end

    private

    def log(content, severity: :info)
      @logger&.public_send(severity) { "<#{stream_name}:#{group_name}> #{content}" }
    end

    def handle_entry(entry)
      instance_exec(entry, &@on_request)
    end

    def handle_exception(exception)
      instance_exec(exception, &@on_error)
    end

    def check_for_valid_configuration!
      raise ConfigurationError, "Stream#on_request must be a lambda or proc" if !@on_request.is_a?(Proc)
      raise ConfigurationError, "Stream#on_error must be a lambda or proc" if !@on_error.is_a?(Proc)
      raise ConnectionError, "Stream is already connected" if @ledger
    end

    def check_for_ledger!
      raise ConnectionError, "Stream has not been set up correctly. Please call Stream#connect first" if @ledger.nil?
    end

    def create_consumers(options)
      @redis.clear_available_consumers

      options[:pool_size].times.map do |index|
        name = "consumer_#{index}"

        consumer = Ledger::Consumer.new(name, redis: @redis, ledger: @ledger, options: options)
        consumer.add_callback(:on_error, self, :handle_exception)
        consumer.add_callback(:on_message, self, :handle_entry)
        consumer.listen

        consumer
      end
    end

    def create_dispatchers(options)
      options[:pool_size].times.map do |index|
        name = "dispatcher_#{index}"

        dispatcher = Dispatcher.new(name, redis: @redis, options: options)
        dispatcher.add_callback(:on_error, self, :handle_exception)
        dispatcher.listen

        dispatcher
      end
    end

    def track_and_send(content, destination_group)
      entry = Entry.new(content: content, source_group: group_name, destination_group: destination_group)

      log("Sending entry #{entry.id} to \"#{entry.destination_group}\" with: #{entry.content}")

      # The entry must be added to the ledger before adding to the stream as that, in testing, the consumers can
      # get ahold of the entry before the ledger has everything ready.
      mailbox = @ledger.store_entry(entry)
      entry = @redis.add_to_stream(entry)

      # The mailbox is a Concurrent::MVar which allows us to wait for data to be added (or timeout)
      # This is handled in Ledger::Consumer
      case (result = mailbox.take(@ledger.options[:entry_timeout]))
      when Stream::Entry
        if result.fulfilled?
          Response.fulfilled(result.content)
        else
          Response.rejected(result.content)
        end
      when StandardError
        Response.rejected(result)
      else
        # Concurrent::MVar::TIMEOUT
        Response.rejected(TimeoutError.new)
      end
    ensure
      @ledger.delete_entry(entry)
    end
  end
end
