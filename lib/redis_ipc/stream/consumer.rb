# frozen_string_literal: true

module RedisIPC
  class Stream
    class Consumer
      DEFAULTS = {
        # The number of Consumers to create to process entries for their group. Any consumers created
        # for a group will only process entries for their group
        pool_size: 5,

        # How often does the consumer check for new messages
        execution_interval: 0.01,

        # Controls what queue this consumer processes
        #   :self - Processes the PEL for this consumer. Acts as an endpoint for all entries for this group
        #   :stream - Processes unread entries for the stream. Acts as a dispatcher for all entries in a stream
        queue: :self
      }.freeze

      READ_FROM_PEL = "0"
      READ_FROM_STREAM = ">"

      attr_reader :name, :stream_name, :group_name

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
        @name = name.freeze
        @stream_name = stream.freeze
        @group_name = group.freeze

        validate!

        @options = DEFAULTS.merge(options).freeze
        @redis = Commands.new(stream_name, group_name, redis_options: redis_options)
        @read_id = (@options[:queue] == :self) ? READ_FROM_PEL : READ_FROM_STREAM

        # Used to ensure the task does not finish until all observers does
        @callback_sync = nil

        # This is the workhorse for the consumer
        @task = Concurrent::TimerTask.new(execution_interval: @options[:execution_interval], freeze_on_deref: true) do
          check_for_entries
        end
      end

      #
      # A wrapper for #add_observer that simplifies processing entries by
      # removing the need to have code on every observer to handle nil entries or exceptions
      # If manual exception handling is needed, use #add_observer. Just remember that the result can be nil
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

        case callback_type
        # Ignores exceptions and only calls when it's a success
        when :on_message
          add_observer do |_, entry, exception|
            next if exception || entry.nil?

            handler.call(entry)
          end
        # Ignores successful messages and only calls on exceptions
        when :on_error
          add_observer do |_, _e, exception|
            next unless exception

            handler.call(exception)
          end
        else
          raise ArgumentError, "Invalid callback type #{callback_type} provided. Expected :on_message, or :on_error"
        end
      end

      #
      # Starts checking the stream for new messages
      #
      def listen
        return if @task.running?

        @redis.create_group
        @task.execute

        change_availability

        RedisIPC.logger&.debug { "🔗 '#{name}'" }
        @task
      end

      #
      # Stops checking the stream for new messages
      #
      def stop_listening
        @task.shutdown

        RedisIPC.logger&.debug { "⛓️‍💥 '#{name}'" }

        change_availability

        true
      end

      #
      # The method that is called by the consumer's task.
      #
      # @note This is default functionality. This is expected to be overwritten by other classes
      # @see Dispatcher, Ledger::Consumer
      #
      def check_for_entries
        entry = read_from_stream
      ensure
        acknowledge_and_remove(entry) if entry
      end

      private

      def validate!
        raise ArgumentError, "was created without a name" if name.blank?
        raise ArgumentError, "#{name} was created without a stream name" if stream_name.blank?
        raise ArgumentError, "#{name} was created without a group name" if group_name.blank?
      end

      def reject!(entry)
        # TODO: Send back an error to source_group
        RedisIPC.logger&.error { "❗ '#{name}' - TODO! Rejected #{entry.id}" }
      end

      def change_availability
        return unless @options[:queue] == :self

        if @task.running?
          @redis.consumer_is_available(name)
        else
          @redis.consumer_is_unavailable(name)
        end
      end

      #
      # Reads the latest messages from the PEL (if a consumer) or the group (if a dispatcher)
      # Message entries are then passed to any observers currently registered with the consumer
      #
      # This method does not acknowledge the message in the stream. This must be handled by an observer
      #
      # @return [Entry]
      #
      def read_from_stream
        # Currently, this code only supports reading one message at a time.
        # Support for processing multiple entries could be implemented later for better performance
        response = @redis.read_from_stream(name, @read_id)&.values&.flatten
        return if response.blank?

        Entry.from_redis(*response)
      end

      def acknowledge_and_remove(entry)
        @redis.acknowledge_and_remove(entry.redis_id)
      end
    end
  end
end
