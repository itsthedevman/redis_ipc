# frozen_string_literal: true

module RedisIPC
  class Stream
    class Commands
      REDIS_DEFAULTS = {
        host: ENV.fetch("REDIS_HOST", "localhost"),
        port: ENV.fetch("REDIS_PORT", 6379)
      }.freeze

      READ_FROM_PEL = "0"
      READ_FROM_STREAM = ">"

      attr_reader :stream_name, :group_name, :redis_pool

      def initialize(stream_name, group_name, pool_size: 10, redis_options: {})
        @stream_name = stream_name
        @group_name = group_name

        redis_options = REDIS_DEFAULTS.merge(redis_options)
        @redis_pool = ConnectionPool.new(size: pool_size) { Redis.new(**redis_options) }
      end

      #
      # Gracefully shutdown the redis pool
      #
      def shutdown
        redis_pool.shutdown(&:close)
      end

      #
      # Returns the number of entries in a stream
      #
      # @return [Integer]
      #
      def entries_size
        redis_pool.with { |redis| redis.xlen(stream_name) }
      end

      #
      # Adds an entry to the Stream
      #
      # @param entry [RedisIPC::Stream::Entry]
      #
      # @return [String] Redis's internal Stream entry ID
      #
      def add_to_stream(entry)
        redis_pool.with { |redis| redis.xadd(stream_name, entry.to_h) }
      end

      #
      # Forcefully removes an entry from the stream through acknowledgment and deletion
      #
      # @param id [String] The Redis Stream ID
      #
      def acknowledge_and_remove(id)
        redis_pool.with do |redis|
          begin
            redis.xack(stream_name, group_name, id)
          rescue Redis::CommandError
          end

          begin
            redis.xdel(stream_name, id)
          rescue Redis::CommandError
          end
        end

        nil
      end

      #
      # Checks if the Stream group has been created and creates it if it hasn't
      #
      def create_group
        redis_pool.with do |redis|
          # $ is the last entry in the stream
          redis.xgroup(:create, stream_name, group_name, "0", mkstream: true)
        rescue Redis::CommandError => e
          break if e.message.start_with?("BUSYGROUP")

          raise e
        end
      end

      #
      # Reads an entry into the group using XREADGROUP
      #
      # @param consumer_name [String] The consumer reading the message
      # @param read_id [String] The ID or special ID to start reading from
      # @param count [Integer] The number of entries to read back
      #
      # @return [RedisIPC::Stream::Entry]
      #
      def read_from_stream(consumer_name, read_id)
        response = redis_pool.with do |redis|
          redis.xreadgroup(group_name, consumer_name, stream_name, read_id, count: 1)&.values&.flatten
        end

        return if response.blank?

        Entry.from_redis(*response)
      end

      #
      # Wrapper for #read_from_stream that returns an unread entry
      #
      # @param consumer_name [String] The name of the consumer (Dispatcher) whom is claiming this entry
      #
      # @return [RedisIPC::Stream::Entry]
      #
      def next_unread_entry(consumer_name)
        read_from_stream(consumer_name, READ_FROM_STREAM)
      end

      #
      # Wrapper for #read_from_stream that returns an entry from the consumers PEL
      #
      # @param consumer_name [String] The name of the consumer (Consumer) who has already claimed this entry
      #
      # @return [RedisIPC::Stream::Entry]
      #
      def next_pending_entry(consumer_name)
        read_from_stream(consumer_name, READ_FROM_PEL)
      end

      #
      # Reclaims any entries that have been idle more than min_idle_time
      #
      # @param consumer_name [String] The name of the consumer to claim the entry
      # @param min_idle_time [Integer] The number of seconds the entry has been idle
      # @param count [Integer] The number of entries to read
      #
      def next_reclaimed_entry(consumer_name, min_idle_time: 10.seconds)
        response = redis_pool.with do |redis|
          # "0-0" is a special ID, means at the start
          redis.xautoclaim(
            stream_name, group_name,
            consumer_name,
            min_idle_time.in_milliseconds.to_i,
            "0-0",
            count: 1
          )["entries"].first
        end

        return if response.blank?

        Entry.from_redis(*response)
      end

      #
      # Gets information about the stream's consumers for a given group
      #
      # @param for_group_name [String] The group the consumers belong to
      #
      def consumer_info(for_group_name = group_name, filter_for_consumers = [])
        result = redis_pool.with do |redis|
          redis.xinfo(:consumers, stream_name, for_group_name)
        end

        if filter_for_consumers.present?
          result.select! { |consumer| filter_for_consumers.include?(consumer["name"]) }
        end

        result.index_by { |consumer| consumer["name"] }
      end

      #
      # Claims an entry for a given consumer, adding it to their PEL
      #
      # @param consumer_name [String] The name of the consumer to claim the entry
      # @param entry [RedisIPC::Stream::Entry]
      #
      def claim_entry(consumer_name, entry)
        redis_pool.with do |redis|
          # 0 is minimum idle time
          redis.xclaim(stream_name, group_name, consumer_name, 0, entry.redis_id)
        end
      end

      #
      # Returns all available consumer names for a group.
      # This data is managed by RedisIPC itself. Each consumer in the list is added when they start listening
      # and is removed when they stop listening
      #
      # @param for_group_name [String] The name of group that the consumers belong to
      #
      # @return [Array<String>]
      #
      def available_consumer_names(for_group_name)
        redis_pool.with do |redis|
          # 0 is start index
          # -1 is end index (like array)
          redis.lrange(available_redis_consumers_key(for_group_name), 0, -1)
        end
      end

      #
      # Clears the array of available consumers for this group
      #
      def clear_available_consumers
        redis_pool.with { |redis| redis.del(available_redis_consumers_key) }
      end

      #
      # Makes the consumer available for messages by adding it to the available consumers list
      #
      # @param consumer_name [String] The name of the consumer
      #
      def make_consumer_available(consumer_name)
        redis_pool.with do |redis|
          redis.lpush(available_redis_consumers_key, consumer_name)
        end
      end

      #
      # Makes the consumer unavailable for messages by removing it from the available consumers list
      #
      # @param consumer_name [String] The name of the consumer
      #
      def make_consumer_unavailable(consumer_name)
        redis_pool.with do |redis|
          # 0 is remove all
          redis.lrem(available_redis_consumers_key, 0, consumer_name)
        end
      end

      private

      def available_redis_consumers_key(for_group_name = group_name)
        "#{stream_name}:#{for_group_name}:available_consumers"
      end
    end
  end
end
