# frozen_string_literal: true

module RedisIPC
  class DispatchObserver
    def update(time_of_execution, message, exception)
      raise exception if exception

      binding.pry
    end
  end
end
