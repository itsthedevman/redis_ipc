# frozen_string_literal: true

module RedisIPC
  class Stream
    class Ledger
      #
      # A consumer that only consumes messages that are registered with the ledger
      #
      # @note It felt better to me to let Consumer be generic
      #
      class Consumer < Consumer
        def initialize(*, ledger:, **)
          super(*, **)
          @ledger = ledger
        end

        def check_for_new_messages
          entry = read_from_group

          # Expired? Drop the message
          if (ledger_entry = @ledger[entry]) && ledger_entry.expired?
            @redis.acknowledge_and_remove(entry.id)
            return
          end

          # Pass to any observers
          entry
        end
      end
    end
  end
end
