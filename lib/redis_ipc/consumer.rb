# frozen_string_literal: true

module RedisIPC
  class Consumer
    DEFAULTS = {
      pool_size: 2,
      execution_interval: 0.01,
      read_count: 1
    }.freeze

    attr_reader :name, :stream_name, :group_name, :redis

    delegate :add_observer, :delete_observer, to: :@task

    def initialize(name, stream:, group:, options: {}, redis_options: {})
      @name = name
      @stream_name = stream
      @group_name = group
      @options = normalize_options(options)
      @redis = Redis.new(redis_options)

      # Read the latest message from this consumers Pending Entries List
      @read_group_id = "0"

      # This is the workhorse for the consumer
      @task = Concurrent::TimerTask.new(
        execution_interval: @options[:execution_interval],
        &method(:process_next_message)
      )
    end

    def stop_listening
      @task.shutdown
    end

    def listen
      ensure_consumer_group_exists
      @task.execute
      @task
    end

    def acknowledge(message_id)
      redis.xack(stream_name, group_name, message_id)
    end

    private

    def process_next_message
      # response = { "stream_name" => [["message_id", "destination_group", content]] }
      response = redis.xreadgroup(
        group_name, name, stream_name, @read_group_id,
        count: @options[:read_count]
      )

      debug!(
        name: name, stream: stream_name, group: group_name,
        read_id: @read_group_id, response: response
      )

      return if response.nil?

      # Any observers will receive this Entry instance
      # If no observer reads this message, it will stay in the PEL until timeout
      Entry[*response.values.flatten]
    end

    def ensure_consumer_group_exists
      return if redis.exists?(stream_name)

      redis.xgroup(:create, stream_name, group_name, "$", mkstream: true)
    end

    def normalize_options(opts)
      DEFAULTS.merge(opts)
    end
  end
end
