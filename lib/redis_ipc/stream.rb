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

      @options = OPTION_DEFAULTS.merge(options)
      @redis_options = REDIS_DEFAULTS.merge(redis_options)

      @ledger = Ledger.new(**@options.slice(:entry_timeout, :cleanup_interval))
      @redis = Commands.new(stream_name, group_name, pool_size: @options[:pool_size], redis_options: @redis_options)

      @consumers = create_consumers
      @dispatchers = create_dispatchers

      RedisIPC.logger&.debug {
        "🔗 #{stream_name}:#{group_name} - #{@consumers.size} consumers, #{@dispatchers.size} dispatchers"
      }

      self
    end

    def disconnect
      RedisIPC.logger&.debug { "⛓️‍💥 #{stream_name}:#{group_name}" }

      @consumers.each(&:stop_listening)
      @dispatchers.each(&:stop_listening)
      @redis.shutdown

      @options = nil
      @redis_options = nil

      @ledger = nil
      @consumers = nil
      @dispatchers = nil
      @redis = nil

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

      @redis.add_to_stream(entry.to_h)

      nil
    end

    #
    # @private
    #
    def handle_message(entry)
      return if @ledger[entry]

      on_message.call(entry)
    ensure
      # Acknowledged that we received the ID to remove it from the PEL
      @redis.acknowledge_and_remove(entry.redis_id)
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

      options[:pool_size].times.map do |index|
        consumer = Ledger::Consumer.new(
          "consumer_#{index}",
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
    end

    def create_dispatchers
      options = @options.fetch(:dispatcher, Dispatcher::DEFAULTS)
      consumer_names = @consumers.map(&:name)

      options[:pool_size].times.map do |index|
        dispatcher = Dispatcher.new(
          "dispatcher_#{index}",
          consumer_names,
          stream: stream_name,
          group: group_name,
          ledger: @ledger,
          options: options,
          redis_options: @redis_options
        )

        dispatcher.listen
        dispatcher
      end
    end

    def track_and_send(content, destination_group)
      entry = Entry.new(
        content: content,
        source_group: group_name,
        destination_group: destination_group
      )

      # Track the message for expiration and ensuring it is returned to our consumer
      mailbox = @ledger.add(entry)
      redis_id = @redis.add_to_stream(entry.to_h)

      case (result = mailbox.take(@options[:entry_timeout]))
      when Concurrent::MVar::TIMEOUT
        raise TimeoutError
      when StandardError
        raise result
      else
        result
      end
    ensure
      @ledger.delete(entry)
      @redis.acknowledge_and_remove(redis_id)
    end
  end
end
