# frozen_string_literal: true

module RedisIPC
  class Stream
    class Ledger < Concurrent::Map
      DEFAULTS = {
        entry_timeout: 5, # Seconds
        cleanup_interval: 1 # Seconds
      }.freeze

      class Entry < Data.define(:expires_at, :mailbox)
        def expired?
          Time.current >= expires_at
        end
      end

      attr_reader :options

      def initialize(options = {})
        super()

        @options = DEFAULTS.merge(options)

        @timeout_in_seconds = @options[:entry_timeout].seconds
        @cleanup_task = Concurrent::TimerTask.execute(execution_interval: @options[:cleanup_interval]) do
          each { |id, entry| delete(id) if entry.expired? }
        end
      end

      def [](entry)
        super(entry.id)
      end

      def add(entry)
        raise ArgumentError, "#{entry.id} is already in the ledger" if self[entry]

        mailbox = Concurrent::MVar.new
        self[entry.id] = Ledger::Entry.new(expires_at: @timeout_in_seconds.from_now, mailbox: mailbox)

        mailbox
      end

      def delete(entry)
        super(entry.id)
      end
    end
  end
end
