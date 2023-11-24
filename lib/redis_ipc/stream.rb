# frozen_string_literal: true

module RedisIPC
  class Stream
    REDIS_DEFAULTS = {
      host: ENV.fetch("REDIS_HOST", "localhost"),
      port: ENV.fetch("REDIS_PORT", 6379)
    }.freeze

    class_attribute :stream_name
    class_attribute :group_name

    attr_reader :redis_options, :options, :redis_pool, :consumer_threads

    def initialize(stream, group: nil, options: {}, redis_options: {})
      self.stream_name = stream
      self.group_name = group

      @redis_options = REDIS_DEFAULTS.merge(redis_options)
      @options = {
        # 1ms execution
        consumers: {
          count: 2,
          execution_interval: 0.001
        },
        dispatchers: {
          count: 2
        },
        sender: {
          timeout: 5 # Seconds
        }
      }.deep_merge(options)

      create_redis_pool
      create_consumer_stream
      create_consumer_pool
    end

    #
    # Posts content into the stream for consumers to process
    #
    # @param content [String, Hash, Array] Anything that can be converted to JSON
    #
    # @return [<Type>] <Description>
    #
    def send(content:, to: nil)
      if to.blank?
        raise ArgumentError,
          "A group must be provided to the to: kwarg or provided during initialization as an option"
      end

      promise = Concurrent::Promise.execute do
        consumer_pool.with do |consumer|
          message_id = post_content_to_stream(consumer.id, to, content)

          observer = consumer.add_observer(ResponseObserver.new(message_id))
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
          content: JSON.generate(content)
        })
      end
    end

    def create_redis_pool
      @redis_pool = ConnectionPool.new(size: 5) do
        Redis.new(**redis_options)
      end
    end

    def create_consumer_stream
      redis_pool.with do |redis|
        return if redis.exists?(stream_name)

        redis.xgroup(:create, stream_name, group_name, "$", mkstream: true)
      end
    end

    def create_consumer_pool
      consumer_count = options[:consumers][:count]

      @consumer_pool = ConnectionPool.new(size: consumer_count) do |i|
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
