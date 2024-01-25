# frozen_string_literal: true

describe RedisIPC::Channel do
  include_context "stream"

  describe ".stream" do
    subject(:channel) { Class.new(described_class) }

    it "sets the value" do
      channel.stream "stream_name"
      expect(channel.stream_name).to eq("stream_name")
    end
  end

  describe ".group" do
    subject(:channel) { Class.new(described_class) }

    it "sets the value" do
      channel.group "group_name"
      expect(channel.group_name).to eq("group_name")
    end
  end

  describe "Sending and receiving" do
    let(:channel_1a) do
      Class.new(described_class) do
        stream "shared_stream"
        group "endpoint_1"

        event :echo, params: [:message] do
          raise "Blank message" if params[:message].blank?

          params[:message]
        end
      end
    end

    let(:channel_1b) { Class.new(channel_1a) }

    let(:channel_2a) do
      # Inheritance supported
      Class.new(channel_1a) do
        group "endpoint_2"
      end
    end

    let(:channel_2b) { Class.new(channel_2a) }

    before do
      # Testing multiple instances for the same group
      channel_1a.connect(**redis_commands_opts)
      channel_1b.connect(**redis_commands_opts)
      channel_2a.connect(**redis_commands_opts)
      channel_2b.connect(**redis_commands_opts)
    end

    describe ".trigger_event" do
      context "when the event fulfills the request" do
        it "returns a fulfilled event that contains the result" do
          result = channel_1a.trigger_event(:echo, params: {message: "Hello world!"}, target: :endpoint_2)

          expect(result).to be_instance_of(RedisIPC::Response)
          expect(result.fulfilled?).to be true
          expect(result.rejected?).to be false
          expect(result.value).to eq("Hello world!")
        end
      end

      context "when the event raises an error" do
        it "returns an rejected entry that contains the error" do
          result = channel_2b.trigger_event(:echo, params: {message: ""}, target: :endpoint_1)

          expect(result).to be_instance_of(RedisIPC::Response)
          expect(result.fulfilled?).to be false
          expect(result.value).to be_nil

          expect(result.rejected?).to be true
          expect(result.reason).to include("Blank message")
        end
      end
    end
  end
end
