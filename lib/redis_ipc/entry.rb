# frozen_string_literal: true

module RedisIPC
  #
  # Represents an entry in the Redis Stream
  #
  class Entry < Data.define(:id, :status, :content, :destination_group)
    def self.from_redis(id, data)
      new(id: id, **data.symbolize_keys)
    end

    def initialize(id: nil, status: "pending", **)
      super(id: id, status: status, **)
    end

    def to_h
      super.tap do |hash|
        hash.delete(:id) if id.blank?
      end
    end
  end
end
