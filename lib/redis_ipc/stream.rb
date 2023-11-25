# frozen_string_literal: true

module RedisIPC
  class Stream
    class_attribute :stream_name
    class_attribute :group_name

    attr_reader :redis_options, :options, :redis_pool, :consumer_threads

    def initialize(stream, group: nil, options: {}, redis_options: {})
      self.stream_name = stream
      self.group_name = group

      @redis_options = RedisIPC::REDIS_DEFAULTS.merge(redis_options)
      @options = {
        consumers: Consumer::DEFAULTS,
        dispatchers: {
          count: 2
        },
        sender: {
          timeout: 5 # Seconds
        }
      }.deep_merge(options)

      create_redis_pool
      create_consumer_pool
    end

    def send(content:, to:)
      promise = Concurrent::Promise.execute do
        consumer_pool.with do |consumer|
          message_id = post_content_to_stream(consumer.id, to, content)

          observer = consumer.add_observer(ResponseObserver.new(message_id))

          # The observer holds onto a MVar that stores the message
          # This blocks until the message comes back or timeout is returned
          result = observer.take(options[:sender][:timeout])

          # Ensure this observer is removed so it doesn't keep processing
          consumer.delete_observer(observer)

          # Failed to get a message back
          raise TimeoutError if result == MVar::TIMEOUT
        end

        result
      end

      promise.value
    end

    private

    def post_content_to_stream(sending_consumer_id, destination_group, content)
      redis_pool.with do |redis|
        redis.xadd(@stream, {
          dispatch: {sender: sending_consumer_id, destination: destination_group},
          content: content
        })
      end
    end

    def create_redis_pool
      @redis_pool = ConnectionPool.new(size: 5) do
        Redis.new(**redis_options)
      end
    end

    def create_consumer_pool
      pool_size = options[:consumers][:pool_size]

      @consumer_pool = ConnectionPool.new(size: pool_size) do |i|
        consumer = Consumer.new(
          "consumer_#{i}",
          group: group_name,
          options: options[:consumers],
          redis_options: redis_options
        )

        consumer.listen(:pending)
        consumer
      end
    end

    def create_dispatchers
      dispatcher_count = options[:dispatchers][:count]

      @dispatchers = dispatcher_count.times do |i|
        Dispatcher.new(
          "dispatcher_#{i}",
          group: group_name,
          options: options[:consumers],
          redis_options: redis_options
        )
      end
    end
  end
end
