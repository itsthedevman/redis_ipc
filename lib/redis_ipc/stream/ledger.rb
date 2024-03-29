# frozen_string_literal: true

module RedisIPC
  class Stream
    class Ledger
      def initialize
        @inner = {}
        @mutex = Mutex.new
      end

      def add(request)
        @mutex.synchronize { @inner[request.id] = Concurrent::MVar.new }
      end

      def remove(request)
        @mutex.synchronize { @inner.delete(request.id) }
      end
    end
  end
end
