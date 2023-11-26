# frozen_string_literal: true

module RedisIPC
  class Consumer
    DEFAULTS = {
      pool_size: 2,
      execution_interval: 0.01,
      read_count: 1
    }.freeze

    attr_reader :name, :stream_name, :group_name, :redis

    alias_method :id, :name

    delegate :add_observer, :delete_observer, to: :@task

    def initialize(name, stream:, group:, options: {}, redis_options: {})
      @name = name
      @stream_name = stream
      @group_name = group
      @options = normalize_options(options)
      @redis = Redis.new(redis_options)
      @read_group_id = nil

      @task = Concurrent::TimerTask.new(execution_interval: @options[:execution_interval]) do
        response = redis.xreadgroup(
          group_name, name, stream_name, @read_group_id,
          count: @options[:read_count]
        )

        debug!(
          name: name, stream: stream_name, group: group_name,
          read_id: @read_group_id, response: response
        )

        next if response.nil?

        # { "stream_name" => [["message_id", content]] }
        # Only reading one message at a time
        (message_id, content) = response.values.flatten

        {message_id: message_id, content: content}
      end
    end

    def stop_listening
      @task.shutdown
    end

    def listen(type)
      @read_group_id = type_to_id(type)
      raise ArgumentError, "Invalid type provided to Consumer#listen" if @read_group_id.nil?

      ensure_consumer_group_exists
      @task.execute
      @task
    end

    def acknowledge(message_id)
      redis.xack(stream_name, group_name, message_id)
    end

    private

    def ensure_consumer_group_exists
      return if redis.exists?(stream_name)

      redis.xgroup(:create, stream_name, group_name, "$", mkstream: true)
    end

    def normalize_options(opts)
      DEFAULTS.merge(opts)
    end

    def type_to_id(type)
      case type
      when :unread
        ">"
      when :pending
        "0"
      end
    end
  end
end
