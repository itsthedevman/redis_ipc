# frozen_string_literal: true

module RedisIPC
  class Stream
    class Consumer
      DEFAULTS = {
        pool_size: 2,
        execution_interval: 0.01, # Seconds
        read_group_id: "0" # Read the latest message from this consumers Pending Entries List (PEL)
      }.freeze

      attr_reader :name, :stream_name, :group_name, :redis

      delegate :add_observer, :delete_observer, :count_observers, to: :@task

      #
      # Creates a new Consumer for the given stream.
      # This class is configured to read from its own Pending Entries List by default. This means that
      # this class cannot read messages without them being claimed by this consumer. See Dispatcher
      #
      # @param name [String] The unique name for this consumer to be used in Redis
      # @param stream [String] The name of the Redis Stream
      # @param group [String] The name of the group in the Stream
      # @param options [Hash] Configuration values for the Consumer. See Consumer::DEFAULTS
      # @param redis_options [Hash] The Redis options to be passed into the client. See Stream::REDIS_DEFAULTS
      #
      def initialize(name, stream:, group:, options: {}, redis_options: {})
        @name = name
        @stream_name = stream
        @group_name = group

        validate!

        @options = DEFAULTS.merge(options)
        @redis = Redis.new(redis_options)

        # This is the workhorse for the consumer
        @task = Concurrent::TimerTask.new(execution_interval: @options[:execution_interval]) { handle_message }
      end

      #
      # A wrapper for #add_observer that simplifies processing entries by removing the need to having code on every
      # observer to handle exceptions or not.
      # If manual exception handling is needed, use #add_observer
      #
      # @param callback_type [Symbol] The type of callback to register
      # @param observer [Object] If a block is not provided, this object will have function called on it
      # @param function [Symbol] If a block is not provided, this is the method that will be called on the observer
      # @param &block [Proc] If provided this code will be called as the callback
      #
      # @return [Object] The registered observer
      #
      def add_callback(callback_type, observer = nil, function = :update, &block)
        handler = lambda do |data|
          if block
            yield(data)
          else
            observer.public_send(function, data)
          end
        end

        callback =
          case callback_type
          # Ignores exceptions and only calls when it's a success
          when :on_message
            lambda do |_, entry, exception|
              next if exception

              handler.call(entry)
            end
          # Ignores successful messages and only calls on exceptions
          when :on_error
            lambda do |_, entry, exception|
              next unless exception

              handler.call(exception)
            end
          else
            raise ArgumentError, "Invalid callback type #{callback_type} provided. Expected :on_message, or :on_error"
          end

        add_observer(&callback)
      end

      #
      # Attempts to acknowledge and remove the given Redis message ID from the stream
      #
      # @param id [String] The Redis message ID
      #
      def acknowledge_and_remove(id)
        # Some of the worst code I've ever written, but I want to make real sure that the message has been removed lol
        begin
          redis.xack(stream_name, group_name, id)
        rescue Redis::CommandError
        end

        begin
          redis.xdel(stream_name, id)
        rescue Redis::CommandError
        end

        nil
      end

      #
      # Starts checking the stream for new messages
      #
      def listen
        return if @task.running?

        ensure_group_exists
        @task.execute
        @task
      end

      #
      # Stops checking the stream for new messages
      #
      def stop_listening
        @task.shutdown
      end

      private

      def validate!
        class_name = self.class.name.demodulize

        raise ArgumentError, "#{class_name} was created without a name" if name.blank?
        raise ArgumentError, "#{class_name} #{name} was created without a stream name" if stream_name.blank?
        raise ArgumentError, "#{class_name} #{name} was created without a group name" if group_name.blank?
      end

      #
      # Reads the latest messages from the PEL (if a consumer) or the group (if a dispatcher)
      # Message entries are then passed to any observers currently registered with the consumer
      #
      # This method does not acknowledge the message in the stream. This must be handled by an observer
      #
      def handle_message
        response = redis.xreadgroup(
          group_name, name, stream_name,
          # The ID to read, including special IDs ">".
          # Consumer uses "0" (read my latest entry), Dispatcher uses ">" (read latest unread entry)
          @options[:read_group_id],
          # The number of entries to read.
          # Currently, this code only supports 1. Support for processing multiple entries
          # could be implemented later for better performance
          count: 1
        )&.values&.flatten

        return if response.blank?

        # Pass to any reading observer
        Entry.from_redis(*response)
      end

      def ensure_group_exists
        return if redis.exists?(stream_name)

        redis.xgroup(:create, stream_name, group_name, "$", mkstream: true)
      end
    end
  end
end
