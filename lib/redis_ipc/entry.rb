# frozen_string_literal: true

module RedisIPC
  #
  # Represents an entry in the Redis Stream
  #
  class Entry < Data.define(:message_id, :group, :content)
    def initialize(message_id: nil, **)
      super(message_id: message_id, **)
    end
  end
end
