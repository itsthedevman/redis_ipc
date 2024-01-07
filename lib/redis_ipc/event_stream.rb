# frozen_string_literal: true

module RedisIPC
  class EventStream
    class EventContainer
      attr_reader :params

      def initialize(params, &callback)
        @params_layout = params
        @params = {}
        @callback = callback
      end

      def execute(params)
        @params = params.symbolize_keys.slice(*@params_layout)
        instance_exec(&@callback)
      ensure
        @params = {}
      end
    end

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

    def self.stream(stream_name)
      self.stream_name = stream_name
    end

    def self.group(group_name)
      self.group_name = group_name
    end

    def self.on_error(&block)
      self.on_error = block
    end

    def self.connect
      @stream = Stream.new(stream_name, group_name)
        .on_request(&method(:on_request))
        .on_error { |e| on_error&.call(e) }
        .connect

      true
    end

    def self.disconnect
      @stream&.disconnect
      @stream = nil
    end

    def self.event(event_id, params: {}, &block)
      self.events ||= HashWithIndifferentAccess.new

      events[event_id] = EventContainer.new(params, &block)
    end

    def self.trigger_event(event_id, target:, params: {})
      if @stream.nil?
        raise ConnectionError, "Stream has not been set up correctly. Please call #{self.class}#connect first"
      end

      @stream.send_to_group(to: target, content: {event: event_id, params: params})
    end

    private_class_method def self.on_request(entry)
      event_data = entry.content

      if !event_data.is_a?(Hash)
        @stream.reject_request(entry, content: "Malformed event data. Expected Hash, got #{event_data.class}")
        return
      end

      event_container = events[event_data["event"]]
      if event_container.nil?
        @stream.reject_request(entry, content: "Unsupported event triggered: #{event_data["event"]}")
        nil
      end

      begin
        result = event_container.execute(event_data["params"])
        @stream.fulfill_request(entry, content: result)
      rescue => e
        @stream.reject_request(entry, content: e.message)
      end
    end
  end
end
