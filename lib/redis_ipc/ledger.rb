# frozen_string_literal: true

module RedisIPC
  class Ledger < Concurrent::Map
    def initialize(entry_timeout:, cleanup_interval:, **)
      super(**)

      @timeout_in_seconds = entry_timeout.seconds
      @entry_class = Data.define(:expires_at, :dispatch_to_consumer)
      @cleanup_task = Concurrent::TimerTask.execute(execution_interval: cleanup_interval) do
        # TODO
      end
    end

    def key?(id)
      !!self[id]
    end

    def expired?(id)
      !key?(id)
    end

    def add(id, consumer_name, expires_at: nil)
      raise ArgumentError, "#{id} is already in the ledger" if key?(id)

      self[id] = @entry_class.new(
        expires_at: expires_at || @timeout_in_seconds.from_now,
        dispatch_to_consumer: consumer_name
      )
    end
  end
end
