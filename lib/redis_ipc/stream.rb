# frozen_string_literal: true

module RedisIPC
  class Stream
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

      options = {
        pool_size: 10,
        max_pool_size: nil,
        ledger: Ledger::DEFAULTS,
        consumer: Ledger::Consumer::DEFAULTS,
        dispatcher: Dispatcher::DEFAULTS
      }.merge(options)

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

      @redis = Commands.new(stream_name, group_name, pool_size: max_pool_size, redis_options: redis_options)
      @ledger = Ledger.new(options[:ledger])

      @consumers = create_consumers(options[:consumer])
      @dispatchers = create_dispatchers(options[:dispatcher])

      self
    end

    def disconnect
      @dispatchers.each(&:stop_listening)
      @consumers.each(&:stop_listening)
      @redis.shutdown

      @dispatchers = nil
      @consumers = nil
      @ledger = nil
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

    def create_consumers(options)
      options[:pool_size].times.map do |index|
        consumer = Ledger::Consumer.new(
          "#{group_name}_consumer_#{index}",
          stream: stream_name,
          group: group_name,
          redis: @redis,
          ledger: @ledger,
          options: options
        )

        consumer.add_callback(:on_error, self, :handle_exception)
        consumer.add_callback(:on_message, self, :handle_entry)

        consumer.listen
        consumer
      end
    end

    def create_dispatchers(options)
      options[:pool_size].times.map do |index|
        dispatcher = Dispatcher.new(
          "#{group_name}_dispatcher_#{index}",
          stream: stream_name,
          group: group_name,
          redis: @redis,
          ledger: @ledger,
          options: options
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
      case (result = mailbox.take(@ledger.options[:entry_timeout]))
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
