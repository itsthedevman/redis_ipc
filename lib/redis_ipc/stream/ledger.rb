# frozen_string_literal: true

module RedisIPC
  class Stream
    class Ledger < Concurrent::Map
      DEFAULTS = {
        entry_timeout: 5, # Seconds
        cleanup_interval: 1 # Seconds
      }.freeze

      class Entry < Data.define(:expires_at, :mailbox)
        def initialize(**)
          super(mailbox: Concurrent::MVar.new, **)
        end

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
          each { |id, ledger_entry| delete(id) if ledger_entry.expired? }
        end
      end

      #
      # Fetches a ledger entry by the stream entry's ID
      #
      # @param entry [Stream::Entry]
      #
      # @return [Stream::Ledger::Entry, nil] The associated ledger entry, or nil if not found
      #
      def fetch_entry(entry)
        self[entry.id]
      end

      #
      # Returns true if an entry is in the ledger
      #
      # @param entry [Stream::Entry]
      #
      def entry?(entry)
        !!fetch_entry(entry)
      end

      #
      # Stores an entry in the ledger
      #
      # @param entry [Stream::Entry] The entry to store
      #
      # @return [Concurrent::MVar] The associated mailbox that may or may not contain the response
      #
      def store_entry(entry)
        raise ArgumentError, "#{entry.id} is already in the ledger" if entry?(entry)

        self[entry.id] = Ledger::Entry.new(expires_at: @timeout_in_seconds.from_now)
        self[entry.id].mailbox
      end

      #
      # Removes an entry from the ledger
      #
      # @param entry [Stream::Entry]
      #
      def delete_entry(entry)
        delete(entry.id)
      end
    end
  end
end
