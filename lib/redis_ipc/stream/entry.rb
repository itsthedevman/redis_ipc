# frozen_string_literal: true

module RedisIPC
  class Stream
    #
    # Represents an entry in the Redis Stream
    #
    class Entry < Data.define(:id, :status, :content, :source_group, :destination_group)
      VALID_STATUS = ["pending", "fulfilled", "rejected"].freeze

      #
      # Returns an array containing two items, the Redis message ID and the new Entry instance
      #
      # @param redis_id [String] The message ID Redis uses internally
      # @param data [Hash] The data from the message in Redis
      #
      # @return [<Type>] <description>
      #
      def self.from_redis(redis_id, data)
        [redis_id, new(**data.symbolize_keys)]
      end

      def initialize(id: nil, status: nil, **)
        id ||= SecureRandom.uuid.delete("-")
        status ||= "pending"
        raise ArgumentError, "Status is not one of #{VALID_STATUS}" unless VALID_STATUS.include?(status)

        super(id: id, status: status, **)
      end
    end
  end
end
