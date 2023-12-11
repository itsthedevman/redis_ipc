# frozen_string_literal: true

module RedisIPC
  class Stream
    class Ledger < Concurrent::Map
      class Entry < Data.define(:expires_at, :dispatch_to_consumer)
        def expired?
          Time.current >= expires_at
        end
      end

      def initialize(entry_timeout:, cleanup_interval:, **)
        super(**)

        @timeout_in_seconds = entry_timeout.seconds
        @cleanup_task = Concurrent::TimerTask.execute(execution_interval: cleanup_interval) do
          each { |id, entry| delete(id) if entry.expired? }
        end
      end

      def expired?(id)
        # Expired but hasn't been removed yet. It will be removed shortly, though
        if (entry = self[id])
          entry.expired?
        else
          true
        end
      end

      def add(id, consumer_name, expires_at: nil)
        raise ArgumentError, "#{id} is already in the ledger" if self[id]

        self[id] = Ledger::Entry.new(
          expires_at: expires_at || @timeout_in_seconds.from_now,
          dispatch_to_consumer: consumer_name
        )
      end
    end
  end
end
