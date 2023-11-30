# frozen_string_literal: true

module RedisIPC
  class Consumer
    DEFAULTS = {
      pool_size: 2,
      execution_interval: 0.01, # Seconds
      read_group_id: "0" # Read the latest message from this consumers Pending Entries List (PEL)
    }.freeze

    attr_reader :name, :stream_name, :group_name, :redis

    delegate :add_observer, :delete_observer, to: :@task

    def initialize(name, stream:, group:, options: {}, redis_options: {})
      @name = name
      @stream_name = stream
      @group_name = group
      @options = DEFAULTS.merge(options)
      @redis = Redis.new(redis_options)

      # This is the workhorse for the consumer
      @task = Concurrent::TimerTask.new(execution_interval: @options[:execution_interval]) { process_next_message }
    end

    def stop_listening
      @task.shutdown
    end

    def listen
      ensure_group_exists
      @task.execute
      @task
    end

    def acknowledge(message_id)
      redis.xack(stream_name, group_name, message_id)
    end

    private

    def process_next_message
      # response = ["message_id", { "group": "destination_group", "content": "content"}]
      response = redis.xreadgroup(group_name, name, stream_name, @options[:read_group_id], count: 1)&.values&.flatten
      return if response.blank?

      debug!(name: name, stream: stream_name, group: group_name, response: response)

      # Any observers will receive this Entry instance
      # If no observer reads this message, it will stay in the PEL until timeout
      # NOTE: Options support reading more than message at a time, but this does not!!
      message_id, message = response
      Entry.new(message_id: message_id, **message)
    rescue => e
      error!(e)
      nil
    end

    def ensure_group_exists
      return if redis.exists?(stream_name)

      redis.xgroup(:create, stream_name, group_name, "$", mkstream: true)
    end
  end
end
