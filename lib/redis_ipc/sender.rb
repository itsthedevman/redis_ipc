# frozen_string_literal: true

module RedisIPC
  class Sender
    OPTIONS = {
      pool_size: 10,
      timeout: 5 # Seconds
    }.freeze

    attr_reader :stream_name, :group_name, :options

    def initialize(stream_name, group_name, options: {}, redis_options: {})
      @stream_name = stream_name
      @group_name = group_name
      @options = OPTIONS.merge(options)
      @ledger = Ledger.new(timeout: self.options[:timeout])

      @redis_pool = ConnectionPool.new(size: self.options[:pool_size]) do
        Redis.new(**redis_options)
      end
    end

    def send_with(consumer, destination_group:, content:)
      id = post_to_stream(content: content, destination_group: destination_group)

      ledger.add(id, consumer.name)
      response = wait_for_response(consumer, id)

      # Failed to get a message back
      raise TimeoutError if response == Concurrent::MVar::TIMEOUT
      raise response if response.is_a?(StandardError)

      response
    end

    def respond(entry)
      post_to_stream(entry)
    end

    private

    def post_to_stream(content:, destination_group:)
      @redis_pool.with do |redis|
        # Using Entry to ensure message structure, instead of using a hash directly
        redis.xadd(stream_name, Entry.new(content: content, destination_group: destination_group).to_h)
      end
    end

    def wait_for_response(consumer, waiting_for_id)
      @redis_pool.with do |redis|
        # (Pls correct me if I'm wrong) I don't believe this needs to be a thread-safe variable in this context
        # However, by using MVar I get waiting and timeout support, plus the thread-safety, out of the box.
        # Win win in my book
        response = Concurrent::MVar.new

        observer = consumer.add_observer do |_, entry, exception|
          next unless entry.id == waiting_for_id

          consumer.acknowledge(waiting_for_id)
          response.put(exception || entry.content)
        end

        # The observer holds onto a MVar that stores the message
        # This blocks until the message comes back or timeout is returned
        result = response.take(options[:timeout])

        # Ensure this observer is removed so it doesn't keep processing
        consumer.delete_observer(observer)

        result
      end
    end
  end
end
