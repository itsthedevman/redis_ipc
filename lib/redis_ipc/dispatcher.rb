# frozen_string_literal: true

module RedisIPC
  class Dispatcher
    delegate :redis, to: :@consumer

    def initialize(*)
      @consumer = Consumer.new(*)

      @consumer.listen(:unread)
      @consumer.add_observer(self, :on_message)
    end

    def on_message(_, message, exception)
    end
  end
end
