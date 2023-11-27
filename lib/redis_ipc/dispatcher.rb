# frozen_string_literal: true

module RedisIPC
  class Dispatcher < Consumer
    DEFAULTS = {
      pool_size: 2,
      execution_interval: 0.01,
      read_count: 1
    }.freeze

    def initialize(name, consumer_names = [], **)
      @consumer_names = consumer_names
      super(name, **)

      # Read the latest unread message for the group
      @read_group_id = ">"
    end

    def listen
      super(:unread)
      add_observer(self, :on_message)
    end

    def on_message(_, entry, exception)
      return unless entry.group == group_name

      consumer_name = find_load_balanced_consumer
      redis.xclaim(stream_name, group_name, consumer_name, 0, entry.message_id)
    end

    private

    def find_load_balanced_consumer
      stop_listening
      binding.pry
    end
  end
end
