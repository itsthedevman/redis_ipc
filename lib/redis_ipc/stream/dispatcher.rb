# frozen_string_literal: true

module RedisIPC
  class Stream
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

      def initialize(name, consumer_names = [], ledger:, **)
        super(name, options: DEFAULTS, **)

        @consumer_names = consumer_names
        @ledger = ledger
      end

      def consumer_stats
        @redis.consumer_info
          .index_by { |consumer| consumer["name"] }
          .select { |name, _| @consumer_names.include?(name) }
      end

      def check_for_new_messages
        entry = read_from_group

        # Entry received is in response to a previous request but failed to reply back in time.
        ledger_entry = @ledger[entry]

        # Sent to the group for processing by our consumers.
        # This entry is not in response to a previous request sent out by our sender
        is_a_request = ledger_entry.nil? && entry.status == "pending"

        # Response: Entry received in response to a previous request sent out from our sender.
        is_a_response = ledger_entry && ["fulfilled", "rejected"].include?(entry.status)

        RedisIPC.logger&.debug {
          "#{entry.id} - Ledger? #{!ledger_entry.nil?} - Request? #{is_a_request} - Response? - #{is_a_response}"
        }
        return reject!(entry.redis_id) unless is_a_request || is_a_response

        @redis.claim_message(find_load_balanced_consumer, entry.redis_id)
      end

      private

      def reject!(redis_id)
        @redis.acknowledge_and_remove(redis_id)
        nil
      end

      #
      # Load balances the consumers and returns the least busiest
      #
      # @return [Consumer]
      #
      def find_load_balanced_consumer
        # Loading #consumer_stats is an expensive task for Redis
        busy_consumers = consumer_stats

        available_consumers = @consumer_names.sort do |a, b|
          load_balance_consumer(a, b, busy_consumers: busy_consumers)
        end

        available_consumers.first
      end

      def load_balance_consumer(consumer_a_name, consumer_b_name, busy_consumers:)
        consumer_a_info = busy_consumers[consumer_a_name]
        consumer_b_info = busy_consumers[consumer_b_name]

        # Self explanatory
        consumer_a_is_free = consumer_a_info.nil?
        consumer_b_is_free = consumer_b_info.nil?

        return MOVE_AHEAD if consumer_a_is_free
        return MOVE_BEHIND if consumer_b_is_free

        # Sorts if either don't have pending messages
        # Only continues if both consumers have the same number of messages, but greater than 0
        consumer_a_pending = consumer_a_info&.fetch("pending", 0)
        consumer_b_pending = consumer_b_info&.fetch("pending", 0)

        return MOVE_AHEAD if consumer_a_pending.zero?
        return MOVE_BEHIND if consumer_b_pending.zero?
        return MOVE_AHEAD if consumer_a_pending < consumer_b_pending
        return MOVE_BEHIND if consumer_b_pending < consumer_a_pending

        # Sorts if either are inactive
        consumer_a_inactive_time = consumer_a_info&.fetch("inactive", 0)
        consumer_a_is_inactive = consumer_a_inactive_time.zero?
        consumer_a_idle_time = consumer_a_info&.fetch("idle", 0)

        consumer_b_inactive_time = consumer_b_info&.fetch("inactive", 0)
        consumer_b_is_inactive = consumer_b_inactive_time.zero?
        consumer_b_idle_time = consumer_b_info&.fetch("idle", 0)

        return MOVE_AHEAD if consumer_a_is_inactive && consumer_a_idle_time > consumer_b_idle_time
        return MOVE_BEHIND if consumer_b_is_inactive && consumer_b_idle_time > consumer_a_idle_time

        # At this point both consumers have a > 0 inactive time, meaning they are both processing a request
        # and haven't finished yet. Since it isn't possible to calculate how much longer it will take
        # for the consumers to take to finish processing their requests, I have decided to go with sorting
        # base on the idle time instead of inactive time. Bonus, this becomes a "catch-all"

        # This is backwards because consumer_a idle time needs to be greater if this is to move ahead
        consumer_b_idle_time <=> consumer_a_idle_time
      end
    end
  end
end
