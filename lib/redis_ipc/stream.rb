# frozen_string_literal: true

module RedisIPC
  class Stream
    class_attribute :stream_name
    class_attribute :group_name

    class_attribute :on_message
    class_attribute :on_error

    def initialize(stream, group:)
      self.stream_name = stream
      self.group_name = group
    end

    def connect(options: {}, redis_options: {})
      raise ConfigurationError, "Stream#on_message must be a lambda or proc" unless on_message.is_a?(Proc)
      raise ConfigurationError, "Stream#on_error must be a lambda or proc" unless on_error.is_a?(Proc)

      redis_options = RedisIPC::DEFAULTS.merge(redis_options)

      @sender = Sender.new(
        stream_name, group_name,
        options: options.fetch(:sender, {}),
        redis_options: redis_options
      )

      @consumer_names, @consumer_pool = create_consumers(options.fetch(:consumer, {}), redis_options)
      @dispatcher_pool = create_dispatchers(options.fetch(:dispatcher, {}), redis_options)

      self
    end

    def disconnect
      @consumer_pool.shutdown(&:stop_listening)
      @dispatcher_pool.shutdown(&:stop_listening)

      self
    end

    def send(content:, to:)
      if @sender.nil?
        raise ConnectionError, "Stream has not be connected yet. Please call Stream#connect before sending messages"
      end

      promise = Concurrent::Promise.execute do
        @consumer_pool.with do |consumer|
          @sender.send_with(consumer, content: content, destination_group: to)
        end
      end

      # Wait for us to get a message back, or timeout
      promise.wait

      # If it was rejected for any reason, raise it so the caller can handle it
      raise promise.reason if promise.rejected?

      promise.value
    end

    private

    def create_consumers(options, redis_options)
      # This is the first time I've had an opportunity to use an Enumerator like this...
      consumer_names = options[:pool_size].times.map { |i| "consumer_#{i}" }
      name_enumerator = consumer_names.to_enum

      consumer_pool = ConnectionPool.new(size: options[:pool_size]) do
        consumer_name = name_enumerator.next

        consumer = Consumer.new(
          consumer_name,
          stream: stream_name,
          group: group_name,
          ledger: sender.ledger,
          options: options,
          redis_options: redis_options
        )

        consumer.add_callback(:on_message, self, :process_inbound_message)
        consumer.add_callback(:on_error) { |exception| raise "TODO" }
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
          ledger: sender.ledger,
          options: options[:dispatchers],
          redis_options: redis_options
        )

        dispatcher.listen
        dispatcher
      end
    end

    def process_inbound_message(_, entry, exception)
      # This ensures any consumer can be listening for a specific message but also be processing other messages
      # while waiting since inbound messages that are not responses will always have a consumer that isn't one of ours
      return if @consumer_names.include?(entry.return_to_consumer)

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
  end
end
