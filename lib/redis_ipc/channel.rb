# frozen_string_literal: true

#
# Channel is a higher level implementation of Stream that functions based on unique events identifiers.
# It can also be thought of like a private API between n-number of groups.
#
# Example:
#
#
module RedisIPC
  class Channel
    class_attribute :stream_name
    class_attribute :group_name
    class_attribute :on_error
    class_attribute :events

    def self.inherited(sub_class)
      sub_class.stream_name = stream_name
      sub_class.group_name = group_name
      sub_class.events = events.dup
      sub_class.on_error = on_error
    end

    #
    # Sets the name of the Redis Stream to be used to communicate across
    #
    # @param name [String]
    #
    def self.stream(name)
      self.stream_name = name
    end

    #
    # Sets the name of the Redis Consumer Group used by this channel
    #
    # @param name [String]
    #
    def self.group(name)
      self.group_name = name
    end

    def self.channel(name)
      group(name)
    end

    #
    # Sets the on_error callback
    #
    # @param &block [Proc]
    #
    def self.on_error(&block)
      self.on_error = block
    end

    #
    # Connects to the stream and starts processing any requests
    # @see Stream#connect for accepted arguments
    #
    def self.connect(**)
      @stream = Stream.new(stream_name, group_name)
        .on_request(&method(:on_request))
        .on_error { |e| on_error&.call(e) }
        .connect(**)

      true
    end

    #
    # Disconnects from the stream and stops processing requests
    #
    def self.disconnect
      @stream&.disconnect
      @stream = nil
    end

    #
    # Is the channel connected to the stream?
    #
    def self.connected?
      !!@stream&.connected?
    end

    #
    # Defines an event that can be triggered from other groups
    #
    # @param event_id [String, Symbol] A unique name of this event
    # @param params [Array] A list of params that this event is expected
    # @param &block [Proc] The code that is called when the event is triggered
    #
    def self.event(event_id, params: [], &block)
      self.events ||= HashWithIndifferentAccess.new

      events[event_id] = Event.new(params, &block)
    end

    #
    # Triggers an event on the target group using the given params
    #
    # @param event_id [String, Symbol] The unique event name to be called on the target group.
    #
    # @param target [String, Symbol] The name of the target group to call this event on
    # @param params [Hash] The keys and values to be sent into the event
    #
    # @return [RedisIPC::Stream::Entry]
    #
    def self.trigger_event(event_id, target:, params: {})
      if @stream.nil?
        raise ConnectionError, "Stream has not been set up correctly. Please call #{self.class}#connect first"
      end

      @stream.send_to_group(to: target, content: {event: event_id, params: params})
    end

    #
    # @private
    #
    private_class_method def self.on_request(entry)
      event_data = entry.content

      if !event_data.is_a?(Hash)
        @stream.reject_request(
          entry,
          content: "Malformed event data. Expected Hash, got #{event_data.class}"
        )

        return
      end

      event_container = events[event_data["event"]]
      if event_container.nil?
        @stream.reject_request(
          entry,
          content: "Unsupported event triggered: #{event_data["event"]}"
        )

        return
      end

      begin
        result = event_container.execute(event_data["params"])
        @stream.fulfill_request(entry, content: result)
      rescue => e
        @stream.reject_request(entry, content: e.message)
      end
    end

    #
    # Holds event params and the callback
    # This also gives a container for calling said callback
    #
    class Event
      attr_reader :params

      def initialize(params, &callback)
        @params_layout = params
        @callback = callback
        @params = nil
      end

      def execute(params)
        @params = params.with_indifferent_access.slice(*@params_layout)
        instance_exec(&@callback)
      ensure
        @params = nil
      end
    end
  end
end
