# frozen_string_literal: true

module RedisIPC
  class Stream
    OPTION_DEFAULTS = {
      pool_size: 10, # Number of connections for sending
      entry_timeout: 5, # Seconds
      cleanup_interval: 1 # Seconds
    }.freeze

    REDIS_DEFAULTS = {
      host: ENV.fetch("REDIS_HOST", "localhost"),
      port: ENV.fetch("REDIS_PORT", 6379)
    }.freeze

    class_attribute :stream_name
    class_attribute :group_name

    class_attribute :on_message
    class_attribute :on_error

    def initialize(stream, group)
      self.stream_name = stream
      self.group_name = group
    end

    def connect(options: {}, redis_options: {})
      validate!

      RedisIPC.logger&.debug { "Connecting to stream '#{stream_name}' and group '#{group_name}'" }

      @options = OPTION_DEFAULTS.merge(options)
      @redis_options = REDIS_DEFAULTS.merge(redis_options)

      @ledger = Ledger.new(**@options.slice(:entry_timeout, :cleanup_interval))
      @redis_pool = ConnectionPool.new(size: @options[:pool_size]) { Redis.new(**@redis_options) }

      @consumer_names, @consumer_pool = create_consumers
      @dispatcher_pool = create_dispatchers

      RedisIPC.logger&.debug {
        "Connected to stream '#{stream_name}' and group '#{group_name}' with #{@consumer_pool.size} consumers and #{@dispatcher_pool.size} dispatchers"
      }

      self
    end

    def disconnect
      RedisIPC.logger&.debug {
        "Disconnecting from stream '#{stream_name}' and group '#{group_name}'"
      }

      @consumer_pool.shutdown(&:stop_listening)
      @dispatcher_pool.shutdown(&:stop_listening)
      @redis_pool.shutdown(&:close)

      @options = nil
      @consumer_names = []
      @redis_options = nil
      @ledger = nil
      @consumer_pool = nil
      @dispatcher_pool = nil
      @redis_pool = nil

      self
    end

    def send(content:, to:)
      if @ledger.nil?
        raise ConnectionError, "Stream has not been set up correctly. Please call Stream#connect first"
      end

      # Using a Promise because of the functionality it provides which simplifies this code
      promise = Concurrent::Promise.execute { track_and_send(content, to) }

      # Wait for us to get a message back, or timeout
      promise.wait

      # If it was rejected for any reason, raise it so the caller can handle it
      raise promise.reason if promise.rejected?

      promise.value
    end

    #
    # A wrapper for #send, used response to an inbound entry (to:) using the provided content (with:)
    # This method is non-blocking. Any further calls will result in new messages being sent
    # to the source group and immediately dropped
    #
    # @param to [RedisIPC::Stream::Entry] The entry that we are responding to
    # @param with [Any] Provided data will be added as content on the entry
    #
    def respond_to(entry:, content:, status: "fulfilled")
      if @ledger.nil?
        raise ConnectionError, "Stream has not been set up correctly. Please call Stream#connect first"
      end

      entry = entry.with(
        content: content,
        status: status,
        source_group: group_name,
        destination_group: entry.source_group
      )

      add_to_stream(entry)
      nil
    end

    #
    # @private
    #
    def handle_message(redis_id, entry)
      return if @ledger[entry.id]

      on_message.call(entry)
    ensure
      # Acknowledged that we received the ID to remove it from the PEL
      @consumer_pool.with { |c| c.acknowledge_and_remove(redis_id) }
    end

    #
    # @private
    #
    def handle_exception(exception)
      on_error.call(exception)
    end

    private

    def validate!
      raise ConfigurationError, "Stream#on_message must be a lambda or proc" if !on_message.is_a?(Proc)
      raise ConfigurationError, "Stream#on_error must be a lambda or proc" if !on_error.is_a?(Proc)
      raise ConnectionError, "Stream is already connected" if @ledger
    end

    def create_consumers
      options = @options.fetch(:consumer, Consumer::DEFAULTS)
      pool_size = options[:pool_size]

      consumer_names = pool_size.times.map { |i| "consumer_#{i}" }
      consumer_name_pool = Concurrent::Array.new(consumer_names)

      consumer_pool = ConnectionPool.new(size: pool_size) do
        consumer = Ledger::Consumer.new(
          consumer_name_pool.shift,
          stream: stream_name,
          group: group_name,
          ledger: @ledger,
          options: options,
          redis_options: @redis_options
        )

        consumer.add_callback(:on_message, self, :handle_message)

        consumer.listen
        consumer
      end

      # Forces the connection pool to eager load every consumer
      fill_pool(consumer_pool, pool_size)

      [consumer_names, consumer_pool]
    end

    def create_dispatchers
      options = @options.fetch(:dispatcher, Dispatcher::DEFAULTS)
      pool_size = options[:pool_size]

      dispatcher_names = pool_size.times.map { |i| "dispatcher_#{i}" }
      dispatcher_name_pool = Concurrent::Array.new(dispatcher_names)

      # This _didn't_ need to be a ConnectionPool, but I wanted to make it consistent :D
      dispatcher_pool = ConnectionPool.new(size: pool_size) do
        dispatcher = Dispatcher.new(
          dispatcher_name_pool.shift,
          @consumer_names,
          stream: stream_name,
          group: group_name,
          ledger: @ledger,
          options: options,
          redis_options: @redis_options
        )

        dispatcher.listen
        dispatcher
      end

      # Force the connection pool to eager load every dispatcher
      fill_pool(dispatcher_pool, pool_size)

      dispatcher_pool
    end

    def add_to_stream(entry)
      RedisIPC.logger&.debug { "Adding entry to stream #{stream_name}: #{entry.to_h}" }

      @redis_pool.with { |redis| redis.xadd(stream_name, entry.to_h) }
    end

    def track_and_send(content, destination_group)
      entry = Entry.new(
        content: content,
        source_group: group_name,
        destination_group: destination_group
      )

      @consumer_pool.with do |consumer|
        # Track the message for expiration and ensuring it is returned to our consumer
        @ledger.add(entry.id, consumer.name)

        add_to_stream(entry)
        wait_for_fulfillment(entry.id, consumer)
      ensure
        @ledger.delete(entry.id)
      end
    end

    def wait_for_fulfillment(waiting_for_entry_id, consumer)
      # (Pls correct me if I'm wrong) I don't believe this needs to be a thread-safe variable in this context
      # However, by using MVar I get waiting and timeout support, plus the thread-safety, out of the box.
      # Win win in my book
      response = Concurrent::MVar.new

      observer = consumer.add_observer do |_, (_redis_id, entry), exception|
        next unless entry.id == waiting_for_entry_id

        response.put(exception || entry.content)
      end

      # The observer holds onto a MVar that stores the message
      # This blocks until the message comes back or timeout is returned
      case (result = response.take(@options[:entry_timeout]))
      when Concurrent::MVar::TIMEOUT
        raise TimeoutError
      when StandardError
        raise result
      else
        result
      end
    ensure
      # Remove the observer so it stops processing messages
      consumer.delete_observer(observer)

      # Acknowledged that we received the ID to remove it from the PEL
      consumer.acknowledge_and_remove(waiting_for_entry_id)
    end

    #
    # This code is so dirty. I feel dirty for writing this code
    # But questionable decisions aside, this is how I'm forcing the connection pool to fill the
    # available connections without waiting for it to lazy load them
    #
    def fill_pool(pool, pool_size)
      pool.instance_exec(pool_size) do |pool_size|
        consumers = pool_size.times.map { @available.pop(@timeout) }
        consumers.each { |c| @available.push(c) }
      end
    end
  end
end
