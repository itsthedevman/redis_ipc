# frozen_string_literal: true

describe RedisIPC::Sender do
  include_context "stream"

  let!(:consumer) do
    RedisIPC::Consumer.new("test_consumer", stream: stream_name, group: group_name)
  end

  let!(:dispatcher) do
    RedisIPC::Dispatcher.new("test_dispatcher", [consumer.name], stream: stream_name, group: group_name)
  end

  subject(:sender) { described_class.new(stream_name, "other_group") }

  describe "#send_with" do
    context "when a message is sent and responded to" do
      it "returns the entry" do
        # #send_with blocks the current thread
        task = Concurrent::ScheduledTask.execute(0) do
          sender.send_with(consumer, destination_group: group_name, content: "request")
        end

        entry = wait_for_response!(dispatcher)
        expect(entry).not_to be_nil

        puts "ALL: #{redis.keys}"
        expect(entry.source_group).to eq("other_group")
        expect(entry.destination_group).to eq(group_name)
        expect(entry.return_to_consumer).to eq(consumer.name)
        expect(entry.content).to eq("request")

        # TODO: Since the sender XADDs before it inserts into the ledger, the consumer consumes it before the ledger is updated lol

        # Ensure a ledger entry was created
        ledger_key = RedisIPC.ledger_key(stream_name, entry.id)
        puts "KEY: #{ledger_key}. ALL: #{redis.keys}"
        expect(redis.exists?(ledger_key)).to be true

        # Respond to the "request"
        sender.respond(entry.for_response(content: "response"))

        value = task.value(1)
        expect(value).not_to be_nil, task.reason.to_s
        expect(value.content).to eq("response")

        expect(redis.exists?(ledger_key)).to be false
      end
    end

    context "when a message is sent and timed out"
  end
end
