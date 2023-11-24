# frozen_string_literal: true

module RedisIPC
  class Dispatcher
    delegate :redis, to: :@consumer

    def initialize(...)
      @consumer = Consumer.new(...)

      @consumer.listen(:unread)
      @consumer.add_observer(DispatchObserver.new)
    end
  end
end
