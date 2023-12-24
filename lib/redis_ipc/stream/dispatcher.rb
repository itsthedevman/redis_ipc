# frozen_string_literal: true

module RedisIPC
  class Stream
    #
    # A consumer that reads unread entries and assigns them to consumers to be processed
    #
    class Dispatcher < Consumer
      DEFAULTS = {
        # The number of Dispatchers to create
        pool_size: 3,

        # How often should the consumer process entries in seconds
        execution_interval: 0.01
      }.freeze

      def initialize(name, **)
        super(name, options: DEFAULTS, **)

        check_for_consumers!
      end

      #
      # Reads in any unread entries in the stream for the group and dispatches it to a load balanced consumer
      #
      # One dispatcher per group will receive a stream entry, regardless of content. Dispatchers will then
      # ignore all entries that are not
      #
      #
      def check_for_entries
        entry = read_from_stream
        return if invalid?(entry)

        log("Processing entry:\n#{entry}")

        available_consumer = find_load_balanced_consumer
        if available_consumer.nil?
          reject!(entry, reason: "DISPATCH_FAILURE #{group_name}:#{name} failed to find an available consumer")
          return
        end

        log("Dispatched to #{available_consumer}: #{entry.id}")

        @redis.claim_entry(available_consumer, entry)
      end

      private

      # noop. Dispatchers do not need to be made "available"
      def change_availability = nil

      def read_from_stream
        # Along with dispatching the normal unread entries, Dispatchers also implement two failsafe to ensure
        # all entries are processed.
        #
        # Reclaimed entries: Any entry claimed by a consumer but hasn't been processed within a time frame.
        # Unread entries: Standard workflow entries.
        # Pending entries: Any entry claimed by the Dispatcher, but hasn't been dispatched yet.
        #
        # Reclaimed and pending entries should be a rarity, but its better to handle them than to let them sit
        @redis.next_reclaimed_entry(name) || @redis.next_unread_entry(name) || @redis.next_pending_entry(name)
      end

      def available_consumer_names
        @redis.available_consumer_names
      end

      def check_for_consumers!
        return if available_consumer_names.size > 0

        raise ConfigurationError, "No consumers available for #{stream_name}:#{group_name}. Please make sure at least one Consumer is listening before creating any Dispatchers"
      end

      def find_load_balanced_consumer
        consumer_names = available_consumer_names
        busy_consumers = @redis.consumer_info(group_name, filter_for: consumer_names)

        available_consumers = consumer_names.sort do |a, b|
          load_balance_consumer(a, b, busy_consumers: busy_consumers)
        end

        available_consumers.first
      end

      MOVE_AHEAD = -1
      MOVE_BEHIND = 1

      def load_balance_consumer(consumer_a_name, consumer_b_name, busy_consumers:)
        consumer_a_info = busy_consumers[consumer_a_name]
        consumer_b_info = busy_consumers[consumer_b_name]

        # Self explanatory
        consumer_a_is_free = consumer_a_info.nil?
        consumer_b_is_free = consumer_b_info.nil?

        return MOVE_AHEAD if consumer_a_is_free
        return MOVE_BEHIND if consumer_b_is_free

        # Sorts if either don't have pending entries
        # Only continues if both consumers have the same number of entries, but greater than 0
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
