# frozen_string_literal: true

module RedisIPC
  class Stream
    class Commands
      attr_reader :stream_name, :group_name

      def initialize(stream_name, group_name, pool_size: 10, redis_options: {})
        @stream_name = stream_name
        @group_name = group_name
        @redis_pool = ConnectionPool.new(size: pool_size) { Redis.new(**redis_options) }
      end

      def shutdown
        @redis_pool.shutdown(&:close)
      end

      def clear_available_consumers
        @redis_pool.with { |redis| redis.del(available_redis_consumers_key) }
      end

      def add_to_stream(content)
        @redis_pool.with { |redis| redis.xadd(stream_name, content) }
      end

      def acknowledge_and_remove(id)
        @redis_pool.with do |redis|
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

      def create_group
        @redis_pool.with do |redis|
          # $ is the last entry in the stream
          redis.xgroup(:create, stream_name, group_name, "$", mkstream: true)
        rescue Redis::CommandError => e
          break if e.message.start_with?("BUSYGROUP")

          raise e
        end
      end

      def read_from_stream(consumer_name, read_id, count: 1)
        @redis_pool.with do |redis|
          redis.xreadgroup(group_name, consumer_name, stream_name, read_id, count: count)
        end
      end

      def consumer_info(for_group_name = group_name)
        @redis_pool.with do |redis|
          redis.xinfo(:consumers, stream_name, for_group_name)
        end
      end

      def claim_entry(consumer_name, entry)
        @redis_pool.with do |redis|
          # 0 is minimum idle time
          redis.xclaim(stream_name, group_name, consumer_name, 0, entry.redis_id)
        end
      end

      def consumer_names(for_group_name)
        @redis_pool.with do |redis|
          # 0 is start index
          # -1 is end index (like array)
          redis.lrange(available_redis_consumers_key(for_group_name), 0, -1)
        end
      end

      def consumer_is_available(name)
        @redis_pool.with do |redis|
          redis.lpush(available_redis_consumers_key, name)
        end
      end

      def consumer_is_unavailable(name)
        @redis_pool.with do |redis|
          # 0 is remove all
          redis.lrem(available_redis_consumers_key, 0, name)
        end
      end

      private

      def available_redis_consumers_key(for_group_name = group_name)
        "#{stream_name}:#{for_group_name}:available_consumers"
      end
    end
  end
end
