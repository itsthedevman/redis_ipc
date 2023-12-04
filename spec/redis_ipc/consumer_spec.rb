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
    context "when a messages is dispatched to this consumer" do
      it "creates a Entry instance and broadcasts to all observers without acknowledging it" do
        content = Faker::String.random

        id = send_to(consumer, content: content)

        response = nil
        consumer.add_observer do |time, result, exception|
          response = exception || result
          consumer.stop_listening
        end

        task = consumer.listen

        while task.running?
          sleep(0.1)
        end

        expect(response).to be_kind_of(RedisIPC::Entry)
        expect(response.id).to eq(id)
        expect(response.content).to eq(content)

        consumer_info = redis.xinfo(:consumers, stream_name, group_name).find { |c| c["name"] == consumer.name }
        expect(consumer_info).not_to be_nil
        expect(consumer_info["pending"]).to eq(1)
      end
    end
  end
end
