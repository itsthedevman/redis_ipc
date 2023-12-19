# frozen_string_literal: true

module RedisIPC
  class Stream
    OPTION_DEFAULTS = {
      pool_size: 10, # Number of connections for sending
      entry_timeout: 999, # Seconds
      cleanup_interval: 1 # Seconds
    }.freeze

    REDIS_DEFAULTS = {
      host: ENV.fetch("REDIS_HOST", "localhost"),
      port: ENV.fetch("REDIS_PORT", 6379)
    }.freeze

    class_attribute :stream_name
    class_attribute :group_name

    def initialize(stream, group)
      self.stream_name = stream
      self.group_name = group

      @on_message = nil
      @on_error = nil
    end

    def on_message(&block)
      @on_message = block
    end

    def on_error(&block)
      @on_error = block
    end

    def connect(options: {}, redis_options: {})
      validate!

      @options = OPTION_DEFAULTS.merge(options)
      @redis_options = REDIS_DEFAULTS.merge(redis_options)

      @ledger = Ledger.new(**@options.slice(:entry_timeout, :cleanup_interval))
      @redis = Commands.new(
        stream_name, group_name,
        pool_size: @options[:pool_size],
        redis_options: @redis_options
      )

      @consumers = create_consumers
      @dispatchers = create_dispatchers

      self
    end

    def disconnect
      @dispatchers.each(&:stop_listening)
      @consumers.each(&:stop_listening)
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

      response_entry = entry.with(
        content: content,
        status: status,
        source_group: entry.destination_group,
        destination_group: entry.source_group
      )

      @redis.add_to_stream(response_entry.to_h)

      nil
    end

    #
    # @private
    #
    def handle_entry(entry)
      @on_message.call(entry)
    end

    #
    # @private
    #
    def handle_exception(exception)
      @on_error.call(exception)
    end

    private

    def validate!
      raise ConfigurationError, "Stream#on_message must be a lambda or proc" if !@on_message.is_a?(Proc)
      raise ConfigurationError, "Stream#on_error must be a lambda or proc" if !@on_error.is_a?(Proc)
      raise ConnectionError, "Stream is already connected" if @ledger
    end

    def create_consumers
      @redis.clear_available_consumers

      options = @options.fetch(:consumer, Consumer::DEFAULTS)

      options[:pool_size].times.map do |index|
        consumer = Ledger::Consumer.new(
          "#{group_name}_consumer_#{index}",
          stream: stream_name,
          group: group_name,
          ledger: @ledger,
          options: options,
          redis_options: @redis_options
        )

        consumer.add_callback(:on_error, self, :handle_exception)
        consumer.add_callback(:on_message, self, :handle_entry)

        consumer.listen
        consumer
      end
    end

    def create_dispatchers
      options = @options.fetch(:dispatcher, Dispatcher::DEFAULTS)

      options[:pool_size].times.map do |index|
        dispatcher = Dispatcher.new(
          "#{group_name}_dispatcher_#{index}",
          stream: stream_name,
          group: group_name,
          ledger: @ledger,
          options: options,
          redis_options: @redis_options
        )

        dispatcher.add_callback(:on_error, self, :handle_exception)

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
      @redis.add_to_stream(entry.to_h)

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
    end
  end
end
