# frozen_string_literal: true

module RedisIPC
  #
  # Represents an entry in the Redis Stream
  #
  class Entry < Data.define(:message_id, :group, :content)
    alias_method :id, :message_id

    def initialize(message_id: nil, **)
      super(message_id: message_id, **)
    end

    def to_h
      {group: group, content: content}.tap do |hash|
        hash[:message_id] = message_id if message_id.present?
      end
    end
  end
end
