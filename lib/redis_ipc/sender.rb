# frozen_string_literal: true

module RedisIPC
  class Sender
    OPTIONS = {
      timeout: 5 # Seconds
    }.freeze

    def initialize(options:, redis_options:)
      @options = options

      @redis_pool = ConnectionPool.new(size: 5) do
        Redis.new(**redis_options)
      end
    end

    def send_with(consumer, destination_group:, content:)
      message_id = post_content_to_stream(to, content)
      result = wait_for_response(consumer, message_id)

      # Failed to get a message back
      raise TimeoutError if result == MVar::TIMEOUT

      result
    end

    def post_content_to_stream(destination_group, content)
      redis_pool.with do |redis|
        redis.xadd(@stream, Entry.new(group: destination_group, content: content).to_h)
      end
    end

    def wait_for_response(consumer, waiting_for_message_id)
      redis_pool.with do |redis|
        response = Concurrent::MVar.new

        observer = consumer.add_observer do |_, entry, exception|
          raise exception if exception
          next unless entry.message_id == waiting_for_message_id

          consumer.acknowledge(waiting_for_message_id)
          response.put(entry.content)
        end

        # The observer holds onto a MVar that stores the message
        # This blocks until the message comes back or timeout is returned
        result = response.take(options[:sender][:timeout])

        # Ensure this observer is removed so it doesn't keep processing
        consumer.delete_observer(observer)

        result
      end
    end
  end
end
