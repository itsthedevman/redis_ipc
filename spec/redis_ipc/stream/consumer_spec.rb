# frozen_string_literal: true

describe RedisIPC::Stream::Consumer do
  include_context "stream"

  subject(:consumer) do
    described_class.new("test_consumer", stream: stream_name, group: group_name)
  end

  it "is valid" do
    expect { consumer }.not_to raise_error
  end

  context "when created without a name" do
    subject(:consumer) { described_class.new("", stream: stream_name, group: group_name) }

    it "raises an exception" do
      expect { consumer }.to raise_error(ArgumentError, "Consumer was created without a name")
    end
  end

  context "when created without a stream name" do
    subject(:consumer) { described_class.new("test_consumer", stream: "", group: group_name) }

    it "raises an exception" do
      expect { consumer }.to raise_error(ArgumentError, "Consumer test_consumer was created without a stream name")
    end
  end

  context "when created without a group name" do
    subject(:consumer) { described_class.new("test_consumer", stream: stream_name, group: "") }

    it "raises an exception" do
      expect { consumer }.to raise_error(ArgumentError, "Consumer test_consumer was created without a group name")
    end
  end

  describe "#listen" do
    context "when a messages is dispatched to this consumer" do
      it "creates a Entry instance and broadcasts to all observers without acknowledging it" do
        content = Faker::String.random

        (_redis_id, response) = send_and_delegate_to_consumer(consumer, content: content)

        expect(response).to be_kind_of(RedisIPC::Stream::Entry)
        expect(response.content).to eq(content)

        consumer_info = consumer_stats_for(consumer)
        expect(consumer_info).not_to be_nil
        expect(consumer_info["pending"]).to eq(1)
      end
    end
  end

  describe "#add_callback" do
    context "when the callback type is :on_message" do
      it "adds an observer that only receives successful events" do
        consumer.add_callback(:on_message) {}
        expect(consumer.count_observers).to eq(1)
      end
    end

    context "when the callback type is :on_error" do
      it "adds an observer that only receives exception events" do
        consumer.add_callback(:on_error) {}
        expect(consumer.count_observers).to eq(1)
      end
    end
  end

  describe "#acknowledge_and_remove" do
    it "removes the message from the PEL" do
      (redis_id, _entry) = send_and_delegate_to_consumer(consumer, content: "")
      consumer.acknowledge_and_remove(redis_id)

      consumer_info = consumer_stats_for(consumer)
      expect(consumer_info).not_to be_nil
      expect(consumer_info["pending"]).to eq(0)
    end
  end
end
