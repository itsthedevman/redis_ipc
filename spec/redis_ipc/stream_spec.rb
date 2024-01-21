# frozen_string_literal: true

describe RedisIPC::Stream do
  include_context "stream"

  subject(:stream) do
    described_class.new(stream_name, group_name)
  end

  before do
    stream.on_request {}
    stream.on_error {}
  end

  after do
    stream.disconnect if stream.connected?
  end

  describe "#connect" do
    context "when #on_request is nil" do
      before { stream.instance_variable_set(:@on_request, nil) }

      it "raises an error" do
        expect { stream.connect }.to raise_error(
          RedisIPC::ConfigurationError,
          "Stream#on_request must be a lambda or proc"
        )
      end
    end

    context "when #on_error is nil" do
      before { stream.instance_variable_set(:@on_error, nil) }

      it "raises an error" do
        expect { stream.connect }.to raise_error(
          RedisIPC::ConfigurationError,
          "Stream#on_error must be a lambda or proc"
        )
      end
    end

    context "when additional options is provided" do
      let(:consumers) { stream.instance_variable_get(:@consumers) }
      let(:dispatchers) { stream.instance_variable_get(:@dispatchers) }

      it "uses those values" do
        stream.connect(
          consumer: {pool_size: 3},
          dispatcher: {pool_size: 2}
        )

        expect(consumers.size).to eq(3)
        expect(dispatchers.size).to eq(2)
      end
    end
  end

  describe "#connected?" do
    subject(:connected?) { stream.connected? }

    context "when the stream is not connected" do
      it { is_expected.to be false }
    end

    context "when the stream is connected" do
      before { stream.connect }

      it { is_expected.to be true }
    end
  end

  describe "#disconnect" do
    subject(:disconnection_result) { stream.disconnect }

    context "when the stream is connected" do
      before { stream.connect }

      it "stops the connections" do
        expect(stream.instance_variable_get(:@consumers)).not_to be_nil
        expect(stream.instance_variable_get(:@dispatchers)).not_to be_nil
        expect(stream.instance_variable_get(:@ledger)).not_to be_nil

        expect(disconnection_result).to eq(stream)
        expect(stream.connected?).to be false

        expect(stream.instance_variable_get(:@consumers)).to be_nil
        expect(stream.instance_variable_get(:@dispatchers)).to be_nil
        expect(stream.instance_variable_get(:@ledger)).to be_nil
      end
    end

    context "when the stream is not connected" do
      it "does nothing" do
        expect(stream.instance_variable_get(:@consumers)).to be_nil
        expect(stream.instance_variable_get(:@dispatchers)).to be_nil
        expect(stream.instance_variable_get(:@ledger)).to be_nil

        expect(disconnection_result).to eq(stream)
        expect(stream.connected?).to be false

        expect(stream.instance_variable_get(:@consumers)).to be_nil
        expect(stream.instance_variable_get(:@dispatchers)).to be_nil
        expect(stream.instance_variable_get(:@ledger)).to be_nil
      end
    end
  end

  describe "Sending/Receiving" do
    subject(:other_stream) { described_class.new(stream_name, "other_group") }

    before do
      other_stream.on_request do |entry|
        fulfill_request(entry, content: "#{entry.content} back")
      end

      other_stream.connect(**redis_commands_opts)
      stream.connect(**redis_commands_opts)
    end

    after { other_stream.disconnect }

    context "when an entry is sent" do
      it "receives a response" do
        response = stream.send_to_group(content: "Hello", to: "other_group")
        expect(response).to be_instance_of(RedisIPC::Response)
        expect(response.fulfilled?).to be true

        expect(response.value).to eq("Hello back")
      end
    end
  end

  describe "Timeout" do
    context "when an entry times out" do
      before do
        stream.connect(**redis_commands_opts.merge(ledger: {entry_timeout: 0.01}))
      end

      it "raises and removes the entry from Redis" do
        response = stream.send_to_group(content: "Hello", to: "this_group_doesnt_exist")
        expect(response).to be_instance_of(RedisIPC::Response)
        expect(response.fulfilled?).to be false
        expect(response.rejected?).to be true
        expect(response.value).to be_nil

        expect(redis_commands.entries_size).to eq(0)
      end
    end
  end
end
