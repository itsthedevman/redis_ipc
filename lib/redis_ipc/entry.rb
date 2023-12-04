# frozen_string_literal: true

module RedisIPC
  #
  # Represents an entry in the Redis Stream
  #
  class Entry < Data.define(:id, :group, :content, :consumer)
    def initialize(id: nil, consumer: nil, **)
      super(id: id, consumer: consumer, **)
    end

    def to_h
      {group: group, content: content}.tap do |hash|
        hash[:id] = id if id.present?
        hash[:consumer] = consumer if consumer.present?
      end
    end
  end
end
