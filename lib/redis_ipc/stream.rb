# frozen_string_literal: true

module RedisIPC
  class Stream
    REDIS_DEFAULTS = {
      host: ENV.fetch("REDIS_HOST", "localhost"),
      port: ENV.fetch("REDIS_PORT", 6379)
    }.freeze

    class_attribute :stream_name
    class_attribute :group_name

    attr_reader :options, :redis_pool, :consumer_threads

    def initialize(stream, group: nil, options: {}, redis_options: {})
      self.stream_name = stream
      self.group_name = group

      @options = {
        # 1ms execution
        consumers: {
          worker_count: 2,
          execution_interval: 0.001
        },
        dispatchers: {
          worker_count: 2
        },
        sender: {
          destination: nil
        }
      }.deep_merge(options)

      create_redis_pool(REDIS_DEFAULTS.merge(redis_options))
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
      to ||= @options.dig(:sender, :destination)

      if to.blank?
        raise ArgumentError,
          "A group must be provided to the to: kwarg or provided during initialization as an option"
      end


      promise = Concurrent::Promise.execute do
        consumer = nil
        post_content_to_stream(to, content)
      end

      # promise = promise.then do |result|
      #   # Check for timeout or response
      #   # Fullfil if response
      #   # Reject if timeout
      # end

      promise.value
    end

    private

    def create_redis_pool(redis_options)
      pool_size = @options[:consumers][:worker_count]

      @redis_pool =
        ConnectionPool.new(size: pool_size) do
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
      @consumer_pool =
        ConnectionPool.new(size: 5) do
        end
    end

    def post_content_to_stream(sending_consumer_id, destination_group, content)
      redis_pool.with do |redis|
        redis.xadd(@stream, {
          dispatch: {sender: sending_consumer_id, destination: destination_group},
          content: JSON.generate(content)
        })
      end
    end
  end
end
