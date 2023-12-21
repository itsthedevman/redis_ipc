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
      check_for_valid_configuration!

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
      check_for_ledger!

      # Using a Promise because of the functionality it provides which simplifies this code
      promise = Concurrent::Promise.execute { track_and_send(content, to) }

      # Wait for us to get a message back, or timeout
      promise.wait

      # If it was rejected for any reason, raise it so the caller can handle it
      raise promise.reason if promise.rejected?

      promise.value
    end

    def fulfill(entry:, content:)
      check_for_ledger!

      @redis.add_to_stream(entry.fulfilled(content: content))

      nil
    end

    alias_method :respond_to, :fulfill

    def reject(entry:, content:)
      check_for_ledger!

      @redis.add_to_stream(entry.rejected(content: content))

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

    def check_for_valid_configuration!
      raise ConfigurationError, "Stream#on_message must be a lambda or proc" if !@on_message.is_a?(Proc)
      raise ConfigurationError, "Stream#on_error must be a lambda or proc" if !@on_error.is_a?(Proc)
      raise ConnectionError, "Stream is already connected" if @ledger
    end

    def check_for_ledger!
      raise ConnectionError, "Stream has not been set up correctly. Please call Stream#connect first" if @ledger.nil?
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

      # The entry must be added to the ledger before adding to the stream as that, in testing, the consumers can
      # get ahold of the entry before the ledger has everything ready.
      mailbox = @ledger.add(entry)
      @redis.add_to_stream(entry)

      # The mailbox is a Concurrent::MVar which allows us to wait for data to be added (or timeout)
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
