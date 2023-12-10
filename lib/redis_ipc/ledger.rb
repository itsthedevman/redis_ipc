# frozen_string_literal: true

module RedisIPC
  class Ledger < Concurrent::Map
    class Entry < Data.define(:expires_at, :dispatch_to_consumer)
    end

    def initialize(timeout:, **)
      super(**)

      @timeout_in_seconds = timeout.seconds
      # Todo: Cleanup task
    end

    def key?(id)
      !!self[id]
    end

    def expired?(id)
      !key?(id)
    end

    def add(id, consumer_name, expires_at: nil)
      raise ArgumentError, "#{id} is already in the ledger" if key?(id)

      self[id] = Entry.new(
        expires_at: expires_at || @timeout_in_seconds.from_now,
        dispatch_to_consumer: consumer_name
      )
    end
  end
end
