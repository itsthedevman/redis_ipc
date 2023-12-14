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

      def add_to_stream(content)
        RedisIPC.logger&.debug { "✉️  #{content}" }

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
          break if redis.exists?(stream_name)

          redis.xgroup(:create, stream_name, group_name, "$", mkstream: true)
        end
      end

      def read_from_group(consumer_name, read_id, count: 1)
        @redis_pool.with do |redis|
          redis.xreadgroup(group_name, consumer_name, stream_name, read_id, count: count)
        end
      end

      def consumer_info
        @redis_pool.with do |redis|
          redis.xinfo(:consumers, stream_name, group_name)
        end
      end

      def claim_entry(consumer_name, id)
        @redis_pool.with do |redis|
          redis.xclaim(stream_name, group_name, consumer_name, 0, id)
        end
      end
    end
  end
end
