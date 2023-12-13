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

          add_callback(:on_message, self, :handle_message)
        end

        def handle_message(redis_id, entry)
          # Expired? Drop the message and don't pass it along to the observers
          if ledger.expired?(entry.id)
            delete(redis_id)
            return
          end

          entry
        end
      end
    end
  end
end
