# frozen_string_literal: true

module RedisIPC
  class Stream
    class_attribute :stream_name
    class_attribute :group_name

    def initialize(stream, group)
      self.stream_name = stream
      self.group_name = group

      @on_request = nil
      @on_error = nil
    end

    def on_request(&block)
      @on_request = block
      self
    end

    def on_error(&block)
      @on_error = block
      self
    end

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

      @redis = Commands.new(
        stream_name, group_name,
        max_pool_size: max_pool_size,
        redis_options: redis_options,
        **options.slice(:logger, :reset)
      )

      @redis.destroy_group
      @redis.create_group

      @ledger = Ledger.new(options[:ledger])
      @consumers = create_consumers(options[:consumer])
      @dispatchers = create_dispatchers(options[:dispatcher])

      self
    end

    def connected?
      !@consumers.nil? && !@dispatchers.nil?
    end

    def disconnect
      # #connect might've not been called...
      @dispatchers&.each(&:stop_listening)
      @consumers&.each(&:stop_listening)
      @redis&.shutdown

      @dispatchers = nil
      @consumers = nil
      @ledger = nil
      @redis = nil

      self
    end

    def send_to_group(content:, to:)
      check_for_ledger!

      track_and_send(content, to)
    end

    def fulfill_request(entry, content:)
      check_for_ledger!
      @redis.add_to_stream(entry.fulfilled(content: content))

      nil
    end

    def reject_request(entry, content:)
      check_for_ledger!
      @redis.add_to_stream(entry.rejected(content: content))

      nil
    end

    private

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

      # The entry must be added to the ledger before adding to the stream as that, in testing, the consumers can
      # get ahold of the entry before the ledger has everything ready.
      mailbox = @ledger.store_entry(entry)
      @redis.add_to_stream(entry)

      # The mailbox is a Concurrent::MVar which allows us to wait for data to be added (or timeout)
      # This is handled in Ledger::Consumer
      case (result = mailbox.take(@ledger.options[:entry_timeout]))
      when Concurrent::MVar::TIMEOUT
        raise TimeoutError
      when StandardError
        raise result
      else
        result
      end
    ensure
      @ledger.delete_entry(entry)
    end
  end
end
