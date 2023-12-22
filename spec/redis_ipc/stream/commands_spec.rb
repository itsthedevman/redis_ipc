# frozen_string_literal: true

describe RedisIPC::Stream::Commands do
  include_context "stream"

  # redis_commands is defined in stream_context
  # along with a lot of methods used in this spec
  subject(:commands) { redis_commands }

  describe "#entries_size" do
    subject { entries_size }

    context "when there are zero unread entries" do
      it { is_expected.to eq(0) }
    end

    context "when there are one unread entries" do
      before { add_to_stream }

      it { is_expected.to eq(1) }
    end
  end

  describe "#add_to_stream" do
    it "adds the entry to the stream" do
      expect(entries_size).to eq(0)
      expect(add_to_stream).to be_kind_of(RedisIPC::Stream::Entry)
      expect(entries_size).to eq(1)
    end
  end

  describe "#acknowledge_and_remove" do
    context "when the entry is in the stream" do
      it "removes the entry from Redis" do
        entry = add_to_stream
        expect(entries_size).to eq(1)

        expect { redis_commands.acknowledge_and_remove(entry.redis_id) }.not_to raise_error

        expect(entries_size).to eq(0)
      end
    end

    context "when the entry is not in the stream" do
      it "returns nil and does not raise" do
        expect(entries_size).to eq(0)

        expect { redis_commands.acknowledge_and_remove("meh") }.not_to raise_error

        expect(entries_size).to eq(0)
      end
    end

    context "when the entry is in a consumer's PEL" do
      it "removes the entry from Redis" do
        entry = add_to_stream

        next_unread_entry("dispatcher_1")
        expect(entries_size).to eq(1)

        expect { redis_commands.acknowledge_and_remove(entry.redis_id) }.not_to raise_error
        expect(entries_size).to eq(0)
      end
    end
  end

  describe "#create_group" do
    subject(:create_group) { redis_commands.create_group }

    context "when the stream/group does not exist" do
      before { destroy_stream }

      it "creates the stream/group" do
        expect { redis.xinfo(:stream, stream_name) }.to raise_error(Redis::CommandError, "ERR no such key")

        # xgroup :create by itself doesn't always create the group (xinfo raises)
        # Adding an entry makes xinfo happy
        create_group
        add_to_stream

        expect(redis.xinfo(:stream, stream_name)).to include("length" => 1)
      end
    end

    context "when the stream/group exists" do
      it "does nothing" do
        expect { redis.xinfo(:stream, stream_name) }.not_to raise_error
        expect { create_group }.not_to raise_error
      end
    end
  end

  describe "#next_unread_entry" do
    subject(:unread_entry) { redis_commands.next_unread_entry("consumer_1") }

    context "when there is an unread entry" do
      let!(:entry) { add_to_stream }

      it "returns the entry" do
        is_expected.to eq(entry)
      end
    end

    context "when there is not an unread entry" do
      it { is_expected.to be_nil }
    end

    # Testing pending entries to make sure unread entry doesn't return it
    # Is it needed? eh
    context "when there is a pending entry (other consumer)" do
      before do
        entry = add_to_stream
        next_unread_entry("consumer_1") # has to be read before it can be claimed
        claim_entry("consumer_2", entry)
      end

      # The entry is no longer owned by consumer_1 and has been read
      it { is_expected.to be_nil }
    end

    context "when there is a pending entry (same consumer)" do
      before do
        add_to_stream
        next_unread_entry("consumer_1")
      end

      it { is_expected.to be_nil }
    end
  end

  describe "#next_pending_entry" do
    subject(:pending_entry) { redis_commands.next_pending_entry("consumer_1") }

    context "when there is a pending entry" do
      let!(:entry) { add_to_stream }

      # Reading, but not acknowledging, will put the entry in the PEL
      before { next_unread_entry("consumer_1") }

      it "returns the entry" do
        is_expected.to eq(entry)
      end
    end

    context "when there is not a pending entry" do
      it { is_expected.to be_nil }
    end
  end

  describe "#next_reclaimed_entry" do
    subject(:next_reclaimed_entry) { redis_commands.next_reclaimed_entry("reclaimer", min_idle_time: 0) }

    it "claims the message to the dispatcher" do
      expect(entries_size).to eq(0)

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
