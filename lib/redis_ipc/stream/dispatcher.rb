# frozen_string_literal: true

module RedisIPC
  class Stream
    #
    # A consumer that reads unread entries and assigns them to consumers to be processed
    #
    class Dispatcher < Consumer
      DEFAULTS = {
        # The number of Dispatchers to create
        pool_size: 10,

        # How often should the consumer process entries in seconds
        execution_interval: 0.001 # 1ms
      }.freeze

      def initialize(name, **)
        super(name, options: DEFAULTS, **)
      end

      def listen
        check_for_consumers!

        super
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
        return if entry.nil?

        # When the dispatcher reads an entry from the stream, it is added to its PEL. Acknowledge to remove it.
        # Usually, the entry is also deleted but all dispatchers receive the same entry and deleting it causes it
        # to not be passed to the next dispatcher
        if invalid_entry?(entry)
          acknowledge_entry(entry)
          return
        end

        instance_id = entry.pending? ? @instance_id : entry.instance_id

        consumer = find_load_balanced_consumer(instance_id)
        if consumer.nil?
          log("DISPATCH_FAILURE #{group_name}:#{name} failed to find a consumer", severity: :error)

          acknowledge_entry(entry)
          return
        end

        log("Dispatching to #{consumer.name}: #{entry.id}")

        @redis.claim_entry(consumer, entry)
      end

      private

      # Noop. Dispatchers should not appear in the available consumers list
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
        @redis.next_reclaimed_entry(self) || @redis.next_unread_entry(self) || @redis.next_pending_entry(self)
      end

      def consumer_info(instance_id = @instance_id)
        @redis.consumer_info(filter_for: @redis.available_consumer_names(instance_id))
      end

      def check_for_consumers!
        return if consumer_info.size > 0

        raise ConfigurationError, "No consumers available for #{stream_name}:#{group_name}. Please make sure at least one Consumer is listening before creating any Dispatchers"
      end

      def find_load_balanced_consumer(instance_id = @instance_id)
        consumer_info(instance_id).values.min { |a, b| load_balance_consumer(a, b) }
      end

      MOVE_AHEAD = -1
      private_constant :MOVE_AHEAD

      MOVE_BEHIND = 1
      private_constant :MOVE_BEHIND

      def load_balance_consumer(consumer_a, consumer_b)
        # Sorts if either consumer don't have pending entries
        # Only continues if both consumers have the same number of entries, but greater than 0
        consumer_a_pending = consumer_a.pending
        consumer_b_pending = consumer_b.pending

        return MOVE_AHEAD if consumer_a_pending.zero?
        return MOVE_BEHIND if consumer_b_pending.zero?
        return MOVE_AHEAD if consumer_a_pending < consumer_b_pending
        return MOVE_BEHIND if consumer_b_pending < consumer_a_pending

        # Sorts if either consumer are inactive
        consumer_a_is_inactive = consumer_a.inactive.zero?
        consumer_a_idle_time = consumer_a.idle

        consumer_b_is_inactive = consumer_b.inactive.zero?
        consumer_b_idle_time = consumer_b.idle

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
