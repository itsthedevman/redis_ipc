# frozen_string_literal: true

describe RedisIPC::Stream::Commands do
  include_context "stream"

  # redis_commands is defined in stream_context
  # along with a lot of methods used in this spec
  subject(:commands) { redis_commands }

  describe "#unread_entries_size" do
    subject { unread_entries_size }

    context "when there are zero unread entries" do
      it { is_expected.to eq(0) }
    end

    context "when there are one unread entries" do
      before { add_to_stream(entry) }

      it { is_expected.to eq(1) }
    end
  end

  describe "#add_to_stream" do
    it "adds the entry to the stream" do
      expect(unread_entries_size).to eq(0)
      expect(add_to_stream).to be_kind_of(String)
      expect(unread_entries_size).to eq(1)
    end
  end

  describe "#acknowledge_and_remove" do
    context "when the entry is in the stream" do
      it "removes the entry from Redis" do
        entry = add_to_stream
        expect(unread_entries_size).to eq(1)

        expect { redis_commands.acknowledge_and_remove(entry.redis_id) }.not_to raise_error

        expect(unread_entries_size).to eq(0)
      end
    end

    context "when the entry is not in the stream" do
      it "returns nil and does not raise" do
        expect(unread_entries_size).to eq(0)

        expect { redis_commands.acknowledge_and_remove("meh") }.not_to raise_error

        expect(unread_entries_size).to eq(0)
      end
    end

    context "when the entry is in a consumer's PEL" do
      it "removes the entry from Redis" do
        entry = add_to_stream

        next_unread_entry("dispatcher_1")
        expect(unread_entries_size).to eq(1)

        expect { redis_commands.acknowledge_and_remove(entry.redis_id) }.not_to raise_error
        expect(unread_entries_size).to eq(0)
      end
    end
  end

  describe "#next_reclaimed_entry" do
    subject(:next_reclaimed_entry) { redis_commands.next_reclaimed_entry("reclaimer", min_idle_time: 0) }

    it "claims the message to the dispatcher" do
      expect(unread_entries_size).to eq(0)

      entry = add_to_stream

      next_unread_entry("dispatcher_1") # dispatcher_1 takes ownership
      claim_entry("consumer_1", entry) # consumer_1 takes ownership

      # Check make sure things are correct
      expect(consumer_info).to include(
        "dispatcher_1" => hash_including("pending" => 0),
        "consumer_1" => hash_including("pending" => 1)
      )

      expect(next_reclaimed_entry).to eq(entry) # reclaimer takes ownership

      # Double check!
      expect(consumer_info).to include(
        "dispatcher_1" => hash_including("pending" => 0),
        "consumer_1" => hash_including("pending" => 0),
        "reclaimer" => hash_including("pending" => 1)
      )
    end
  end
end
