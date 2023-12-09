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

      validate!

      @options = DEFAULTS.merge(options)
      @redis = Redis.new(redis_options)

      # This is the workhorse for the consumer
      @task = Concurrent::TimerTask.new(execution_interval: @options[:execution_interval]) { process_next_message }
    end

    def acknowledge(id)
      remove_from_ledger(id)
      redis.xack(stream_name, group_name, id)
    end

    def delete(id)
      remove_from_ledger(id)
      redis.xdel(stream_name, id)
    end

    def listen
      return if @task.running?

      ensure_group_exists
      @task.execute
      @task
    end

    def stop_listening
      @task.shutdown
    end

    private

    def validate!
      class_name = self.class.name.demodulize

      raise ArgumentError, "#{class_name} was created without a name" if name.blank?
      raise ArgumentError, "#{class_name} #{name} was created without a stream name" if stream_name.blank?
      raise ArgumentError, "#{class_name} #{name} was created without a group name" if group_name.blank?
    end

    def remove_from_ledger(id)
      redis.del(RedisIPC.ledger_key(stream_name, id))
    end

    def process_next_message
      response = redis.xreadgroup(group_name, name, stream_name, @options[:read_group_id], count: 1)&.values&.flatten
      return if response.blank?

      # Any observers will receive this Entry instance
      # If no observer reads this message, it will stay in the PEL until timeout
      entry = Entry.from_redis(*response)

      # When a message is posted to the stream, an entry in the stream ledger is added with EXPIRE
      # If the key does not exist, the message has expired so do not process it
      if expired?(entry.id)
        delete(entry.id)
        return
      end

      entry
    rescue => e
      RedisIPC.logger&.error(JSON.generate(message: e.message, backtrace: e.backtrace))
      nil
    end

    def ensure_group_exists
      return if redis.exists?(stream_name)

      redis.xgroup(:create, stream_name, group_name, "$", mkstream: true)
    end

    def expired?(id)
      !redis.exists?(RedisIPC.ledger_key(stream_name, id))
    end
  end
end
