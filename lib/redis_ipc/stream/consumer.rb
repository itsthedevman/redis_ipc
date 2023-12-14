# frozen_string_literal: true

module RedisIPC
  class Stream
    class Consumer
      DEFAULTS = {
        pool_size: 2,
        execution_interval: 0.01, # Seconds
        read_group_id: "0" # Read the latest message from this consumers Pending Entries List (PEL)
      }.freeze

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
        @name = name
        @stream_name = stream
        @group_name = group
        @type = self.class.name.demodulize

        validate!

        @options = DEFAULTS.merge(options)
        @redis = Commands.new(stream_name, group_name, redis_options: redis_options)

        # This is the workhorse for the consumer
        @task = Concurrent::TimerTask.new(execution_interval: @options[:execution_interval]) { check_for_new_messages }
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
              next if exception || entry.nil?

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
      # Starts checking the stream for new messages
      #
      def listen
        return if @task.running?

        ensure_group_exists
        @task.execute

        RedisIPC.logger&.debug { "🔗 #{stream_name}:#{group_name} - #{@type} '#{name}'" }

        @task
      end

      #
      # Stops checking the stream for new messages
      #
      def stop_listening
        RedisIPC.logger&.debug { "⛓️‍💥 #{stream_name}:#{group_name} - #{@type} '#{name}'" }
        @task.shutdown
      end

      #
      # The method that is called by the consumer's task.
      # Written this way to allow overwriting by other classes
      #
      def check_for_new_messages
        read_from_group
      end

      private

      def validate!
        raise ArgumentError, "#{@type} was created without a name" if name.blank?
        raise ArgumentError, "#{@type} #{name} was created without a stream name" if stream_name.blank?
        raise ArgumentError, "#{@type} #{name} was created without a group name" if group_name.blank?
      end

      #
      # Reads the latest messages from the PEL (if a consumer) or the group (if a dispatcher)
      # Message entries are then passed to any observers currently registered with the consumer
      #
      # This method does not acknowledge the message in the stream. This must be handled by an observer
      #
      # @return [Entry]
      #
      def read_from_group
        # Currently, this code only supports reading one message at a time.
        # Support for processing multiple entries could be implemented later for better performance
        response = @redis.read_from_group(name, @options[:read_group_id])&.values&.flatten
        RedisIPC.logger&.debug { "📬 #{stream_name}:#{group_name} - #{@type} '#{name}' - #{response}" }
        return if response.blank?

        Entry.from_redis(*response)
      end

      def ensure_group_exists
        @redis.create_group
      end
    end
  end
end
