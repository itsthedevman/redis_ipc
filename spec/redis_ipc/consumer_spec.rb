# frozen_string_literal: true

describe RedisIPC::Consumer do
  include_context "stream"

  subject(:consumer) do
    described_class.new("test_consumer", stream: stream_name, group: group_name)
  end

  it "is valid" do
    expect { consumer }.not_to raise_error
  end

  describe "#listen" do
    context "when the type is :unread" do
      it "reads messages from the group (acts like a Dispatcher)" do
        message_id = redis.xadd(stream_name, {key_1: "value_1"})
        response = wait_for_response!(consumer, :unread)

        expect(response[:message_id]).to eq(message_id)
        expect(response[:content]).to eq("key_1" => "value_1")
      end
    end

    context "when the type is :pending" do
      it "reads messages from the PEL for this consumer" do
        message_id = redis.xadd(stream_name, {key_1: "value_1"})

        # Pretend we're a dispatcher and read incoming messagesQ
        wait_for_response!(consumer, :unread, ack: false)

        # Reading the message above without an ACK will move it into the PEL
        # Allowing us to "dispatch" the message to a consumer
        redis.xclaim(stream_name, group_name, consumer.name, 0, message_id)

        # Which we can now read from that consumer's PEL with an ACK
        response = wait_for_response!(consumer, :pending)

        # And finally validate it
        expect(response[:message_id]).to eq(message_id)
        expect(response[:content]).to eq("key_1" => "value_1")
      end
    end
  end

end
