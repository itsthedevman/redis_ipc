# frozen_string_literal: true

module RedisIPC
  class Stream
    #
    # A consumer that only consumes messages that are registered with the ledger
    #
    # @note It felt better to me to let Consumer be generic
    #
    class LedgerConsumer < Consumer
      def initialize(ledger:, **)
        super(**)
        @ledger = ledger

        add_callback(:on_message, self, :handle_message)
        add_callback(:on_error, self, :handle_exception)
      end

      def handle_message(entry)
      end

      def handle_exception(exception)
      end
    end
  end
end
