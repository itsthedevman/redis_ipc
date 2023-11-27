# frozen_string_literal: true

module RedisIPC
  class Stream
    class_attribute :stream_name
    class_attribute :group_name

    def initialize(stream, group: nil, options: {}, redis_options: {})
      self.stream_name = stream
      self.group_name = group

      redis_options = RedisIPC::DEFAULTS.merge(redis_options)

      @sender = Sender.new(options: options[:sender], redis_options: redis_options)

      consumer_names, @consumer_pool = create_consumers(options[:consumer], redis_options)
      @dispatcher_pool = create_dispatchers(options[:dispatcher], redis_options, consumer_names)
    end

    def send(content:, to:)
      promise = Concurrent::Promise.execute do
        @consumer_pool.with do |consumer|
          @sender.send_with(consumer, destination_group: to, content: content)
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
      pool_size = options[:pool_size]

      # This is the first time I've had an opportunity to use an Enumerator (Or even Iterator in Rust) like this...
      consumer_names = pool_size.times.map { |i| "consumer_#{i}" }
      name_enumerator = consumer_names.to_enum

      consumer_pool = ConnectionPool.new(size: pool_size) do
        consumer_name = name_enumerator.next

        consumer = Consumer.new(consumer_name, group: group_name, options: options, redis_options: redis_options)
        consumer.listen
        consumer
      end

      [consumer_names, consumer_pool]
    end

    def create_dispatchers(options, redis_options, consumer_names)
      pool_size = options[:pool_size]

      # Copy pasta. I guess this is now the second time I've used the #to_enum method. lol
      dispatcher_names = pool_size.times.map { |i| "dispatcher_#{i}" }
      name_enumerator = dispatcher_names.to_enum

      # This _didn't_ need to be a ConnectionPool, but I wanted to make it consistent :D
      ConnectionPool.new(size: pool_size) do
        dispatcher_name = name_enumerator.next

        dispatcher = Dispatcher.new(dispatcher_name, consumer_names,
          group: group_name,
          options: options[:dispatchers],
          redis_options: redis_options)

        dispatcher.listen
        dispatcher
      end
    end
  end
end
