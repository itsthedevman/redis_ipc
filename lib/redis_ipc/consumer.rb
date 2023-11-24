# frozen_string_literal: true

module RedisIPC
  class Consumer
    attr_reader :name, :stream, :group, :redis

    alias_method :id, :name

    delegate :add_observer, to: :@task

    def initialize(name, stream:, group:, options: {}, redis_options: {})
      @name = name
      @stream = stream
      @group = group
      @options = options
      @redis = Redis.new(redis_options)
      @task = nil
    end

    def dispose
      @task.shutdown
    end

    def listen(type)
      id = type_to_id(type)
      raise ArgumentError, "Invalid type provided to Consumer#listen" if id.nil?

      @task = Concurrent::TimerTask.new(execution_interval: options[:execution_interval]) do
        redis.xreadgroup(group, name, stream, id)
      end

      @task.execute
      @task
    end

    private

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
