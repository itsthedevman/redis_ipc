# frozen_string_literal: true

describe RedisIPC::Stream do
  include_context "stream"

  subject(:stream) do
    described_class.new(stream_name, group_name)
  end

  before do
    stream.on_message = lambda {}
    stream.on_error = lambda {}
  end

  describe "#connect" do
    context "when #on_message is nil" do
      before { stream.on_message = nil }

      it "raises an error" do
        expect { stream.connect }.to raise_error(
          RedisIPC::ConfigurationError,
          "Stream#on_message must be a lambda or proc"
        )
      end
    end

    context "when #on_error is nil" do
      before { stream.on_error = nil }

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

  describe "#disconnect"

  describe "Sending/Receiving" do
    subject!(:other_stream) do
      stream = described_class.new(stream_name, "other_group")
      stream.on_error = -> {}

      stream.on_message = lambda do |entry|
        puts "RECEIVED ENTRY: #{entry}"
        stream.respond_to(entry: entry, content: "#{entry.content} back")
      end

      stream.connect
      stream
    end

    # The main stream connection
    before { stream.connect }

    context "when a valid message is sent" do
      it "receives a response" do
        expect(stream.send(content: "Hello", to: "other_group")).to eq("Hello back")
      end
    end
  end
end

# stream = RedisIPC::Stream.new(stream_name, group: group_name)

# stream.on_message = lambda do |content|
# end

# stream.on_error = lambda do |content|
# end

# stream.connect
