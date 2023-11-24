# frozen_string_literal: true

module RedisIPC
  class ResponseObserver
    def initialize(waiting_for_message_id)
      @message_id = waiting_for_message_id
      @response = Concurrent::MVar.new
    end

    def update(time_of_execution, message, exception)
      raise exception if exception

      binding.pry
    end
  end
end
