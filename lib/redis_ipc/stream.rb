# frozen_string_literal: true

module RedisIPC
  class Stream
    OPTION_DEFAULTS = {
      pool_size: 10, # Number of connections for sending
      entry_timeout: 5, # Seconds
      cleanup_interval: 0.1 # Seconds
    }.freeze

    REDIS_DEFAULTS = {
      host: ENV.fetch("REDIS_HOST", "localhost"),
      port: ENV.fetch("REDIS_PORT", 6379)
    }.freeze

    class_attribute :stream_name
    class_attribute :group_name

    class_attribute :on_message
    class_attribute :on_error

    def initialize(stream, group:)
      self.stream_name = stream
      self.group_name = group
    end

    def connect(options: {}, redis_options: {})
      validate!

      options = OPTION_DEFAULTS.merge(options)
      redis_options = REDIS_DEFAULTS.merge(redis_options)

      @ledger = Ledger.new(**options.slice(:entry_timeout, :cleanup_interval))
      @redis_pool = ConnectionPool.new(size: options[:pool_size]) { Redis.new(**redis_options) }

      @consumer_names, @consumer_pool = create_consumers(options.fetch(:consumer, {}), redis_options)
      @dispatcher_pool = create_dispatchers(options.fetch(:dispatcher, {}), redis_options)

      self
    end

    def disconnect
      @consumer_pool.shutdown(&:stop_listening)
      @dispatcher_pool.shutdown(&:stop_listening)
      @redis_pool.shutdown(&:close)

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
      promise = Concurrent::Promise.execute { send_and_wait(content, to) }

      # Wait for us to get a message back, or timeout
      promise.wait

      # If it was rejected for any reason, raise it so the caller can handle it
      raise promise.reason if promise.rejected?

      promise.value
    end

    #
    # @private
    #
    def handle_inbound_messages(_completed_at, entry, exception)
      # This ensures any consumer can be listening for a specific message but also be processing other messages
      # while waiting since inbound messages that are not responses will always have a consumer that isn't one of ours
      return if @consumer_names.include?(entry.return_to_consumer)

      # My brain wants this to be the other way around, but I refuse to use unless with a block
      if exception
        on_error.call(entry, exception)
      else
        on_message.call(entry)
      end

      # Acknowledge we processed the request
      @consumer_pool.with do |consumer|
        consumer.acknowledge(entry.id)
      end
    end

    private

    def validate!
      raise ConfigurationError, "Stream#on_message must be a lambda or proc" if !on_message.is_a?(Proc)
      raise ConfigurationError, "Stream#on_error must be a lambda or proc" if !on_error.is_a?(Proc)
      raise ConnectionError, "Stream is already connected" if @ledger
    end

    def create_consumers(options, redis_options)
      # This is the first time I've had an opportunity to use an Enumerator like this...
      consumer_names = options[:pool_size].times.map { |i| "consumer_#{i}" }
      name_enumerator = consumer_names.to_enum

      consumer_pool = ConnectionPool.new(size: options[:pool_size]) do
        consumer_name = name_enumerator.next

        consumer = LedgerConsumer.new(
          consumer_name,
          stream: stream_name,
          group: group_name,
          ledger: @ledger,
          options: options,
          redis_options: redis_options
        )

        consumer.add_observer(self, :handle_inbound_messages)
        consumer.listen

        consumer
      end

      [consumer_names, consumer_pool]
    end

    def create_dispatchers(options, redis_options)
      # Copy pasta. I guess this is now the second time I've used the #to_enum method. lol
      dispatcher_names = options[:pool_size].times.map { |i| "dispatcher_#{i}" }
      name_enumerator = dispatcher_names.to_enum

      # This _didn't_ need to be a ConnectionPool, but I wanted to make it consistent :D
      ConnectionPool.new(size: options[:pool_size]) do
        dispatcher_name = name_enumerator.next

        dispatcher = Dispatcher.new(
          dispatcher_name, @consumer_names,
          stream: stream_name,
          group: group_name,
          ledger: @ledger,
          options: options[:dispatchers],
          redis_options: redis_options
        )

        dispatcher.listen
        dispatcher
      end
    end

    def send_and_wait(content, destination_group)
      @consumer_pool.with do |consumer|
        id = post_to_stream(content: content, destination_group: to)
        response = wait_for_response(id, consumer)

        # Failed to get a message back
        raise TimeoutError if response == Concurrent::MVar::TIMEOUT
        raise response if response.is_a?(StandardError)

        response
      end
    end

    def post_to_stream(content:, destination_group:)
      @redis_pool.with do |redis|
        # Using Entry to ensure message structure, instead of using a hash directly
        redis.xadd(
          stream_name,
          Entry.new(content: content, destination_group: destination_group).to_h
        )
      end
    end

    def wait_for_response(waiting_for_id, consumer)
      @ledger.add(waiting_for_id, consumer.name)

      @redis_pool.with do |redis|
        # (Pls correct me if I'm wrong) I don't believe this needs to be a thread-safe variable in this context
        # However, by using MVar I get waiting and timeout support, plus the thread-safety, out of the box.
        # Win win in my book
        response = Concurrent::MVar.new

        observer = consumer.add_observer do |_, entry, exception|
          next unless entry.id == waiting_for_id

          consumer.acknowledge(waiting_for_id)
          response.put(exception || entry.content)
        end

        # The observer holds onto a MVar that stores the message
        # This blocks until the message comes back or timeout is returned
        result = response.take(options[:timeout])

        # Ensure this observer is removed so it doesn't keep processing
        consumer.delete_observer(observer)

        result
      end
    end
  end
end
