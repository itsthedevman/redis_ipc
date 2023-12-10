# frozen_string_literal: true

module RedisIPC
  #
  # A consumer that reads unread messages and assigns them to consumers to be processed
  #
  class Dispatcher < Consumer
    DEFAULTS = {
      pool_size: 2,
      execution_interval: 0.01, # Seconds
      read_group_id: ">" # Read the latest unread message for the group
    }.freeze

    MOVE_AHEAD = -1
    MOVE_BEHIND = 1

    def initialize(name, consumer_names = [], **)
      @consumer_names = consumer_names
      super(name, options: DEFAULTS, **)

      add_callback(:on_message, self, :handle_message)
      add_callback(:on_error, self, :handle_exception)
    end

    def consumer_stats
      redis.xinfo(:consumers, stream_name, group_name)
        .index_by { |consumer| consumer["name"] }
        .select { |name, _| @consumer_names.include?(name) }
    end

    def handle_message(entry)
      return unless accept?(entry)

      consumer_name = entry.dispatch_to_consumer || find_load_balanced_consumer
      redis.xclaim(stream_name, group_name, consumer_name, 0, entry.id)
    end

    def handle_exception(exception)
      # TODO: Write to logger, respond back with rejection message - right?
    end

    private

    def accept?(entry)
      # Only process messages for our group since all dispatchers receive the same message
      true if entry.destination_group == group_name

      # Now things get fun. In order to be accepted, an entry must be in one of two "states". Any expired responses
      #
      # Request: an entry that was sent to us to process and then respond to.
      #          These entries must have a status of "pending" and do not exist in the ledger
      #
      # Response: an entry that was sent to us in response to an entry we sent to them
      #           These entries must have a status of "fullfilled" or "rejected" and have an entry in the ledger
      #
      # return unless entry.dispatch_to_consumer.nil? || @consumer_names.include?(entry.dispatch_to_consumer)
    end

    #
    # Load balances the consumers and returns the least busiest
    #
    # @return [Consumer]
    #
    def find_load_balanced_consumer
      busy_consumers = consumer_stats

      available_consumers =
        @consumer_names.sort do |consumer_a_name, consumer_b_name|
          consumer_a_info = busy_consumers[consumer_a_name]
          consumer_b_info = busy_consumers[consumer_b_name]

          # Self explanatory
          consumer_a_is_free = consumer_a_info.nil?
          consumer_b_is_free = consumer_b_info.nil?

          next MOVE_AHEAD if consumer_a_is_free
          next MOVE_BEHIND if consumer_b_is_free

          # Sorts if either don't have pending messages
          # Only continues if both consumers have the same number of messages, but greater than 0
          consumer_a_pending = consumer_a_info&.dig("pending") || 0
          consumer_b_pending = consumer_b_info&.dig("pending") || 0

          next MOVE_AHEAD if consumer_a_pending.zero?
          next MOVE_BEHIND if consumer_b_pending.zero?
          next MOVE_AHEAD if consumer_a_pending < consumer_b_pending
          next MOVE_BEHIND if consumer_b_pending < consumer_a_pending

          # Sorts if either are inactive
          consumer_a_inactive_time = consumer_a_info&.dig("inactive") || 0
          consumer_a_is_inactive = consumer_a_inactive_time.zero?
          consumer_a_idle_time = consumer_a_info&.dig("idle") || 0

          consumer_b_inactive_time = consumer_b_info&.dig("inactive") || 0
          consumer_b_is_inactive = consumer_b_inactive_time.zero?
          consumer_b_idle_time = consumer_b_info&.dig("idle") || 0

          next MOVE_AHEAD if consumer_a_is_inactive && consumer_a_idle_time > consumer_b_idle_time
          next MOVE_BEHIND if consumer_b_is_inactive && consumer_b_idle_time > consumer_a_idle_time

          # At this point both consumers have a > 0 inactive time, meaning they are both processing a request
          # and haven't finished yet. Since it isn't possible to calculate how much longer it will take
          # for the consumers to take to finish processing their requests, I have decided to go with sorting
          # base on the idle time instead of inactive time. Bonus, this becomes a "catch-all"

          # This is backwards because consumer_a idle time needs to be greater if this is to move ahead
          consumer_b_idle_time <=> consumer_a_idle_time
        end

      available_consumers.first
    end
  end
end
