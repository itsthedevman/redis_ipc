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

      class ConsumerProxy < Data.define(:name, :pending, :idle, :inactive)
        def initialize(name:, pending: 0, idle: 0, inactive: 0)
          super(name: name, pending: pending, idle: idle, inactive: inactive)
        end
      end

      attr_reader :stream_name, :group_name, :redis_pool, :logger, :instance_id

      #
      # A centralized location that holds all of the various Redis commands needed to interact with a stream
      #
      # @param stream_name [String] The name of the Stream
      # @param group_name [String] The group name to use within the Stream
      # @param max_pool_size [Integer] The maximum number of Redis connections
      # @param logger [nil, Logger] A logger instance. If provided, logs will be appended on commands
      # @param redis_options [Hash] The connection options passed into the Redis client
      #
      def initialize(stream_name, group_name, max_pool_size: 10, logger: nil, redis_options: {})
        # A unique ID to the stream instance. This allows the same stream group be created across multiple
        # instances, or processes, without having inbound responses being dispatched to an instance that
        # isn't currently tracking that request/response
        @instance_id = SecureRandom.uuid.delete("-")[0..5]

        @stream_name = stream_name
        @group_name = group_name

        raise ArgumentError, "Stream name cannot be blank" if stream_name.blank?
        raise ArgumentError, "Group name cannot be blank" if group_name.blank?

        @logger = logger

        redis_options = REDIS_DEFAULTS.merge(redis_options)
        @redis_pool = ConnectionPool.new(size: max_pool_size) { Redis.new(**redis_options) }
      end

      def log(content, severity: :debug)
        @logger&.public_send(severity) { "<#{stream_name}:#{group_name}:#{instance_id}> #{content}" }
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
      # @return [RedisIPC::Stream::Entry]
      #
      def add_to_stream(entry)
        redis_id = redis_pool.with { |redis| redis.xadd(stream_name, entry.to_h) }
        log("add_to_stream - Entry ID: #{entry.id}, Redis ID: #{redis_id}")

        entry.with(redis_id: redis_id)
      end

      #
      # Acknowledges the entry in the PEL
      #
      # @param entry [RedisIPC::Stream::Entry]
      #
      def acknowledge_entry(entry)
        redis_pool.with do |redis|
          suppress(Redis::CommandError) do
            redis.xack(stream_name, group_name, entry.redis_id)
          end
        end
      end

      #
      # Removes the entry from the stream
      #
      # @param entry [RedisIPC::Stream::Entry]
      #
      def delete_entry(entry)
        redis_pool.with do |redis|
          suppress(Redis::CommandError) do
            redis.xdel(stream_name, entry.redis_id)
          end
        end
      end

      #
      # Checks if the Stream group has been created and creates it if it hasn't
      #
      def create_group
        redis_pool.with do |redis|
          suppress(Redis::CommandError) do
            redis.xgroup(:create, stream_name, group_name, "$", mkstream: true)
          end
        end
      end

      #
      # Removes the group from the stream
      #
      def destroy_group
        redis_pool.with do |redis|
          suppress(Redis::CommandError) do
            redis.xgroup(:destroy, stream_name, group_name)
          end

          redis.del(available_consumers_key)
        end
      end

      #
      # Deletes the stream from Redis
      #
      def delete_stream
        redis_pool.with do |redis|
          redis.del(stream_name)
        end
      end

      #
      # Creates a consumer in the stream
      #
      # @param consumer [RedisIPC::Stream::Consumer] The consumer processing this request
      #
      def create_consumer(consumer)
        redis_pool.with do |redis|
          redis.xgroup(:createconsumer, stream_name, group_name, consumer.name)
        end
      end

      #
      # Deletes a consumer from the stream
      #
      # @param consumer [RedisIPC::Stream::Consumer] The consumer processing this request
      #
      def delete_consumer(consumer)
        redis_pool.with do |redis|
          redis.xgroup(:delconsumer, stream_name, group_name, consumer.name)
        end
      end

      #
      # Reads an entry into the group using XREADGROUP
      #
      # @param consumer_name [String] The consumer reading the entry
      # @param read_id [String] The ID or special ID to start reading from
      # @param count [Integer] The number of entries to read back
      #
      # @return [RedisIPC::Stream::Entry]
      #
      def read_from_stream(consumer, read_id, block: 500)
        # To avoid this key becoming stale while the instance is still running.
        set_expiry(available_consumers_key, ttl: 1.day)

        result = redis_pool.with do |redis|
          redis.xreadgroup(
            group_name, consumer.name, stream_name, read_id,
            count: 1,
            block: block
          )&.values&.flatten
        end

        return if result.blank?

        Entry.from_redis(*result)
      end

      #
      # Wrapper for #read_from_stream that returns an unread entry
      #
      # @param consumer [RedisIPC::Stream::Consumer] The dispatcher whom is claiming this entry
      #
      # @return [RedisIPC::Stream::Entry]
      #
      def next_unread_entry(consumer, **)
        read_from_stream(consumer, READ_FROM_STREAM, **)
      end

      #
      # Wrapper for #read_from_stream that returns an entry from the consumers PEL
      #
      # @param consumer [RedisIPC::Stream::Consumer] The consumer who has already claimed this entry
      #
      # @return [RedisIPC::Stream::Entry]
      #
      def next_pending_entry(consumer, **)
        read_from_stream(consumer, READ_FROM_PEL, **)
      end

      #
      # Reclaims any entries that have been idle more than min_idle_time
      #
      # @param consumer [RedisIPC::Stream::Consumer] The consumer processing this request to claim the entry
      # @param min_idle_time [Integer] The number of seconds the entry has been idle
      # @param count [Integer] The number of entries to read
      #
      def next_reclaimed_entry(consumer, min_idle_time: 10.seconds)
        result = redis_pool.with do |redis|
          # "0-0" is a special ID, means at the start
          redis.xautoclaim(
            stream_name, group_name,
            consumer.name,
            min_idle_time.in_milliseconds.to_i,
            "0-0",
            count: 1
          )["entries"].first
        end

        return if result.blank?

        Entry.from_redis(*result)
      end

      #
      # Claims an entry for a given consumer, adding it to their PEL
      #
      # @param consumer [Consumer] The consumer to claim the entry
      # @param entry [RedisIPC::Stream::Entry]
      #
      def claim_entry(consumer, entry)
        result = redis_pool.with do |redis|
          # 0 is minimum idle time
          redis.xclaim(stream_name, group_name, consumer.name, 0, entry.redis_id)&.first
        end

        return if result.blank?

        Entry.from_redis(*result)
      end

      #
      # Gets information about the stream's consumers for a given group
      #
      # @param for_group_name [String] The group the consumers belong to
      #
      def consumer_info(for_group_name = group_name, filter_for: nil)
        result = redis_pool.with do |redis|
          redis.xinfo(:consumers, stream_name, for_group_name)
        end

        result = result.map { |r| ConsumerProxy.new(**r.symbolize_keys) }

        if filter_for.is_a?(Array)
          result.select! { |consumer| filter_for.include?(consumer.name) }
        end

        result.index_by(&:name)
      end

      #
      # Clears the array of available consumers for this group
      #
      def clear_available_consumers
        redis_pool.with { |redis| redis.del(available_consumers_key) }
      end

      #
      # Returns all available consumer names for a group.
      # Each consumer in the list is added when they start listening
      # and is removed when they stop listening
      #
      # @param for_group_name [String] The name of group that the consumers belong to
      #
      # @return [Array<String>]
      #
      def available_consumer_names
        redis_pool.with do |redis|
          # 0 is start index
          # -1 is end index (like array)
          redis.lrange(available_consumers_key, 0, -1)
        end
      end

      def consumer_available?(consumer)
        result = redis_pool.with do |redis|
          # redis-rb does not have internal support for lpos. However, they do delegate missing methods
          redis.lpos(available_consumers_key, consumer.name, "RANK", 1)
        end

        !result.nil?
      end

      #
      # Makes the consumer available for receiving entries by adding it to the available consumers list
      #
      # @param consumer_name [String] The name of the consumer
      #
      def make_consumer_available(consumer)
        return if consumer_available?(consumer)

        redis_pool.with do |redis|
          redis.lpush(available_consumers_key, consumer.name)
        end

        true
      end

      #
      # Makes the consumer unavailable for receiving entries by removing it from the available consumers list
      #
      # @param consumer_name [String] The name of the consumer
      #
      def make_consumer_unavailable(consumer)
        return unless consumer_available?(consumer)

        redis_pool.with do |redis|
          # 0 is remove all
          redis.lrem(available_consumers_key, 0, consumer.name)
        end

        true
      end

      private

      def available_consumers_key
        "#{stream_name}:#{group_name}:#{@instance_id}:consumers"
      end

      def set_expiry(key, ttl: 1.second)
        redis_pool.with do |redis|
          redis.expire(key, ttl)
        end
      end
    end
  end
end
