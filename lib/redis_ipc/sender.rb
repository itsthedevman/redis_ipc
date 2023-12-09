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

      @redis_pool = ConnectionPool.new(size: self.options[:pool_size]) do
        Redis.new(**redis_options)
      end
    end

    def send_with(consumer, destination_group:, content:)
      entry = Entry.new(
        content: content,
        source_group: group_name,
        destination_group: destination_group,
        return_to_consumer: consumer.name
      )

      entry_id = post_to_stream(entry)
      response = wait_for_response(consumer, entry_id)

      # Failed to get a message back
      raise TimeoutError if response == Concurrent::MVar::TIMEOUT

      response
    end

    def respond(entry)
      post_to_stream(entry, with_ledger: false)
    end

    private

    def post_to_stream(entry, with_ledger: true)
      @redis_pool.with do |redis|
        id = redis.xadd(stream_name, entry.to_h)

        # Creates an "entry" in the ledger.
        # All this does at this moment is track if the message is active (key exists) or expired (key does not exist)
        if with_ledger
          ledger_key = RedisIPC.ledger_key(stream_name, id)
          redis.set(ledger_key, "", ex: options[:timeout])
          puts "KEY: #{ledger_key}. ALL: #{redis.keys}"
        end

        id
      end
    end

    def wait_for_response(consumer, waiting_for_id)
      @redis_pool.with do |redis|
        # (Pls correct me if I'm wrong) I don't believe this needs to be a thread-safe variable in this context
        # However, by using MVar I get waiting and timeout support, plus the thread-safety, out of the box.
        # Win win in my book
        response = Concurrent::MVar.new

        observer = consumer.add_observer do |_, entry, exception|
          raise exception if exception
          next unless entry.id == waiting_for_id

          consumer.acknowledge(waiting_for_id)
          response.put(entry.content)
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
