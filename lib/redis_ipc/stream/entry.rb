# frozen_string_literal: true

module RedisIPC
  class Stream
    #
    # Represents an entry in the Redis Stream
    #
    class Entry < Data.define(:id, :redis_id, :status, :content, :source_group, :destination_group)
      VALID_STATUS = [
        STATUS_PENDING = "pending",
        STATUS_FULFILLED = "fulfilled",
        STATUS_REJECTED = "rejected"
      ].freeze

      #
      # Returns an array containing two items, the Redis entry ID and the new Entry instance
      #
      # @param redis_id [String] The entry ID Redis uses internally
      # @param data [Hash] The data from the entry in Redis
      #
      # @return [Entry]
      #
      def self.from_redis(redis_id, data)
        new(redis_id: redis_id, **data.symbolize_keys)
      end

      #
      # @param id [NilClass, String] The ID for this entry. Note, this is not the Redis stream ID (that's redis_id)
      # @param redis_id [nil, String] The ID for the entry in the stream. This is generated by Redis
      # @param status [String] The status of the entry. See VALID_STATUS
      # @param content [Object] The data to sent in the entry
      # @param source_group [String] The name of the group that sent the entry. Used for responding back to entries
      # @param destination_group [String] The group that will receive this entry
      #
      def initialize(id: nil, redis_id: nil, status: nil, **)
        id ||= SecureRandom.uuid.delete("-")
        status ||= STATUS_PENDING
        raise ArgumentError, "Status is not one of #{VALID_STATUS}" unless VALID_STATUS.include?(status)

        super(id: id, redis_id: redis_id, status: status, **)
      end

      def to_h
        super.except(:redis_id)
      end

      def fulfilled(content:)
        with(
          content: content,
          status: STATUS_FULFILLED,
          source_group: destination_group,
          destination_group: source_group
        )
      end

      def rejected(content:)
        with(
          content: content,
          status: STATUS_REJECTED,
          source_group: destination_group,
          destination_group: source_group
        )
      end
    end
  end
end
