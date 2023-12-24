# frozen_string_literal: true

module RedisIPC
  class Stream
    class Consumer
      DEFAULTS = {
        # The number of Consumers to create to process entries for their group.
        # Any consumers created for a group will only process entries for their group
        pool_size: 10,

        # How often does the consumer check for new entries
        execution_interval: 0.01
      }.freeze

      attr_reader :name

      delegate :stream_name, :group_name, to: :@redis
      delegate :add_observer, :delete_observer, :count_observers, to: :@task

      #
      # Creates a new Consumer for the given stream.
      # This class is configured to read from its own Pending Entries List by default. This means that
      # this class cannot read entries without them being claimed by this consumer. See Dispatcher
      #
      # @param name [String] The unique name for this consumer to be used in Redis
      # @param redis [RedisIPC::Stream::Commands] The redis commands instance used by Stream
      # @param options [Hash] Configuration values for the Consumer. See Consumer::DEFAULTS
      #
      def initialize(name, redis:, options: {})
        @name = name.freeze
        @redis = redis

        check_for_valid_configuration!

        @redis.create_consumer(name)
        @options = DEFAULTS.merge(options).freeze
        @logger = @redis.logger

        # This is the workhorse for the consumer
        @task = Concurrent::TimerTask.new(
          execution_interval: @options[:execution_interval],
          freeze_on_deref: true
        ) { check_for_entries }
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
            next if entry.nil? || exception

            handler.call(entry)
          end
        # Ignores successful entries and only calls on exceptions
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
      # Returns true if the consumer is listening for entries
      #
      # @return [Boolean]
      #
      def listening?
        @task.running?
      end

      #
      # Starts checking the stream for new entries
      #
      def listen
        return if listening?

        @task.execute
        change_availability

        @task
      end

      #
      # Stops checking the stream for new entries
      #
      def stop_listening
        @task.shutdown

        change_availability
        true
      end

      #
      # Returns the inspected object
      #
      # @return [String]
      #
      def inspect
        "#<#{self.class}:0x#{object_id} name=\"#{name}\" stream_name=\"#{stream_name}\" group_name=\"#{group_name}\" listening=#{listening?}>"
      end

      #
      # The method that is called by the consumer's task.
      #
      # @note This is default functionality. This is expected to be overwritten by other classes
      # @see Dispatcher, Ledger::Consumer
      #
      def check_for_entries
        entry = read_from_stream
        return if invalid?(entry)

        log("Processing entry:\n#{entry}")
        entry
      ensure
        acknowledge_and_remove(entry) if entry
      end

      private

      def log(content)
        @logger&.debug("<#{stream_name}:#{group_name}:#{name}> #{content}")
      end

      def check_for_valid_configuration!
        raise ArgumentError, "was created without a name" if name.blank?
        raise ArgumentError, "#{name} was created without a stream name" if stream_name.blank?
        raise ArgumentError, "#{name} was created without a group name" if group_name.blank?
      end

      def invalid?(entry)
        entry.nil? || entry.destination_group != group_name
      end

      def reject!(entry, reason:)
        @redis.add_to_stream(entry.rejected(content: reason))
        nil
      end

      def change_availability
        if listening?
          @redis.make_consumer_available(name)
        else
          @redis.make_consumer_unavailable(name)
        end
      end

      def read_from_stream
        @redis.next_pending_entry(name)
      end

      def acknowledge_and_remove(entry)
        @redis.acknowledge_and_remove(entry.redis_id)
      end
    end
  end
end
