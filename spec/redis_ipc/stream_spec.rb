# frozen_string_literal: true

describe RedisIPC::Stream do
  include_context "stream"

  subject(:stream) do
    described_class.new(stream_name, group_name)
  end

  before do
    stream.on_message {}
    stream.on_error {}
  end

  after do
    stream.disconnect if stream.connected?
  end

  describe "#connect" do
    context "when #on_message is nil" do
      before { stream.instance_variable_set(:@on_message, nil) }

      it "raises an error" do
        expect { stream.connect }.to raise_error(
          RedisIPC::ConfigurationError,
          "Stream#on_message must be a lambda or proc"
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
        options = {
          consumer: {pool_size: 3},
          dispatcher: {pool_size: 2}
        }

        stream.connect(options: options)

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
    subject!(:other_stream) { described_class.new(stream_name, "other_group") }

    before do
      other_stream.on_message do |entry|
        other_stream.respond_to(entry: entry, content: "#{entry.content} back")
      end

      other_stream.on_error do |exception|
        puts(message: exception.message, backtrace: exception.backtrace)
      end

      other_stream.connect(logger: logger)
      stream.connect(logger: logger)
    end

    after { other_stream.disconnect }

    context "when a valid entry is sent" do
      it "receives a response" do
        expect(stream.send(content: "Hello", to: "other_group")).to eq("Hello back")
      end
    end
  end
end
