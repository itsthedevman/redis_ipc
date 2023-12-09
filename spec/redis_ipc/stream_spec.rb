# frozen_string_literal: true

describe RedisIPC::Stream do
  include_context "stream"

  subject(:stream) do
    described_class.new(stream_name, group: group_name)
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
      let(:consumer_pool) { stream.instance_variable_get(:@consumer_pool) }
      let(:dispatcher_pool) { stream.instance_variable_get(:@dispatcher_pool) }

      it "uses those values" do
        options = {
          consumer: {pool_size: 3},
          dispatcher: {pool_size: 2}
        }

        stream.connect(options: options)

        expect(consumer_pool.size).to eq(3)
        expect(dispatcher_pool.size).to eq(2)
      end
    end
  end

  describe "#disconnect"

  describe "Sending/Receiving" do
    subject(:other_stream) do
      stream = RedisIPC::Stream.new(stream_name, group: "other_group")
      stream.on_error = -> {}

      stream.on_message = lambda do |entry|
        stream.send(content: entry.for_response(content: "#{entry.content} back"))
      end

      stream.connect
      stream
    end

    context "when a message is sent" do
      it "receives the message" do
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
