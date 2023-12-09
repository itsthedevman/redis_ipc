# frozen_string_literal: true

module RedisIPC
  #
  # Represents an entry in the Redis Stream
  #
  class Entry < Data.define(:id, :source_group, :destination_group, :content, :return_to_consumer)
    def self.from_redis(id, data)
      new(id: id, **data.symbolize_keys)
    end

    def initialize(id: nil, return_to_consumer: nil, **)
      super(id: id, return_to_consumer: return_to_consumer, **)
    end

    def response?
      id.present?
    end

    def to_h
      super.tap do |hash|
        hash.delete(:id) if id.blank?
        hash.delete(:return_to_consumer) if return_to_consumer.blank?
      end
    end

    def for_response(content: nil)
      with(
        source_group: destination_group,
        destination_group: source_group,
        content: content
      )
    end
  end
end
