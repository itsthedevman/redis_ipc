# frozen_string_literal: true

module RedisIPC
  class Dispatcher < Consumer
    DEFAULTS = {
      pool_size: 2,
      execution_interval: 0.01, # Seconds
      read_group_id: ">" # Read the latest unread message for the group
    }.freeze

    def initialize(name, consumer_names = [], **)
      @consumer_names = consumer_names
      super(name, options: DEFAULTS, **)

      add_observer(self, :on_message)
    end

    def on_message(_, entry, exception)
      return unless entry.group == group_name

      consumer_name = find_load_balanced_consumer
      redis.xclaim(stream_name, group_name, consumer_name, 0, entry.message_id)
    end

    private

    def find_load_balanced_consumer
      busy_consumers = redis.xinfo(:consumers, stream_name, group_name)
        .index_by { |consumer| consumer["name"] }
        .select { |name, _| @consumer_names.include?(name) }

      available_consumers =
        @consumer_names.sort do |consumer_a_name, consumer_b_name|
          consumer_a_info = busy_consumers[consumer_a_name]
          consumer_b_info = busy_consumers[consumer_b_name]

          # Since nil <=> nil is nil, and it needs to be -1, 0, 1
          consumer_a_is_free = consumer_a_info.nil? ? 0 : 1
          consumer_b_is_free = consumer_b_info.nil? ? 0 : 1

          debug!(a: {name: consumer_a_name, free: consumer_a_is_free == 0}, b: {name: consumer_b_name, free: consumer_b_is_free == 0})

          if consumer_a_is_free + consumer_b_is_free > 0
            # -1 if consumer_a is free
            # 1 if consumer_a is busy
            next consumer_a_is_free <=> consumer_b_is_free
          end

          # Number of pending messages
          consumer_a_pending = consumer_a_info&.dig("pending") || 0
          consumer_b_pending = consumer_b_info&.dig("pending") || 0

          debug!(a: {name: consumer_a_name, pending: consumer_a_pending}, b: {name: consumer_b_name, pending: consumer_b_pending})
          if consumer_a_pending != consumer_b_pending
            # -1 if consumer_a has less pending messages than consumer_b
            # 1 if consumer_a has more pending messages than consumer_b
            next consumer_a_pending <=> consumer_b_pending
          end

          consumer_a_time = (consumer_a_info&.dig("idle") || 0) + (consumer_a_info&.dig("inactive") || 0)
          consumer_b_time = (consumer_b_info&.dig("idle") || 0) + (consumer_b_info&.dig("inactive") || 0)

          debug!(a: {name: consumer_a_name, time: consumer_a_time}, b: {name: consumer_b_name, time: consumer_b_time})

          # -1 if consumer_a has been idle/inactive for less time than consumer_b
          # 1 if consumer_a has been idle/inactive for more time than consumer_b
          consumer_a_time <=> consumer_b_time
        end

      debug!(busy: busy_consumers, available: available_consumers)

      available_consumers.first
    end
  end
end
