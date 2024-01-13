# frozen_string_literal: true

describe RedisIPC::Channel do
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
          raise "Blank message" if params[:message].blank?

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
      context "when the event fulfills the request" do
        it "returns a fulfilled event that contains the result" do
          result = event_stream_1.trigger_event(:echo, params: {message: "Hello world!"}, target: :endpoint_2)

          expect(result).to be_instance_of(RedisIPC::Response)
          expect(result.fulfilled?).to be true
          expect(result.rejected?).to be false
          expect(result.value).to eq("Hello world!")
        end
      end

      context "when the event raises an error" do
        it "returns an rejected entry that contains the error" do
          result = event_stream_1.trigger_event(:echo, params: {message: ""}, target: :endpoint_2)

          expect(result).to be_instance_of(RedisIPC::Response)
          expect(result.fulfilled?).to be false
          expect(result.value).to be_nil

          expect(result.rejected?).to be true
          expect(result.reason).to eq("Blank message")
        end
      end
    end
  end
end
