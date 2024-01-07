# frozen_string_literal: true

describe RedisIPC::EventStream do
  include_context "stream"

  describe ".stream" do
    subject(:stream) { Class.new(described_class) }

    it "sets the value" do
      stream.stream "stream_name"
      expect(stream.stream_name).to eq("stream_name")
    end
  end

  describe ".group" do
    subject(:stream) { Class.new(described_class) }

    it "sets the value" do
      stream.group "group_name"
      expect(stream.group_name).to eq("group_name")
    end
  end

  describe "Sending and receiving" do
    let(:event_stream_1) do
      Class.new(described_class) do
        stream "shared_stream"
        group "endpoint_1"

        event :echo, params: [:message] do
          raise "No message" if params[:message].blank?

          params[:message]
        end
      end
    end

    let(:event_stream_2) do
      # Inheritance supported
      Class.new(event_stream_1) do
        group "endpoint_2"
      end
    end

    before do
      event_stream_1.connect
      event_stream_2.connect
    end

    after do
      event_stream_1.disconnect
      event_stream_2.disconnect
    end

    describe ".trigger_event" do
      context "Success" do
        it "successfully calls the event and returns the result" do
          result = event_stream_1.trigger_event(:echo, params: {message: "Hello world!"}, target: :endpoint_2)

          expect(result).to be_instance_of(RedisIPC::Stream::Entry)
          expect(result.fulfilled?).to be true
          expect(result.rejected?).to be false
          expect(result.content).to eq("Hello world!")
        end
      end

      context "when the event rejects being triggered" do
        it "returns an rejected entry that contains the error" do
          result = event_stream_1.trigger_event(:echo, target: :endpoint_2)

          expect(result).to be_instance_of(RedisIPC::Stream::Entry)
          expect(result.fulfilled?).to be false
          expect(result.rejected?).to be true
          expect(result.content).to eq("No message")
        end
      end
    end
  end
end
