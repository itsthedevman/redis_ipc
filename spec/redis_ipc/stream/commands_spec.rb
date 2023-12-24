# frozen_string_literal: true

describe RedisIPC::Stream::Commands do
  # Most of these methods used in this spec are defined in here
  include_context "stream"

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
      before { redis_commands.delete_stream }

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

  describe "#destroy_group"

  describe "#delete_stream"

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

    it "claims the entry to the dispatcher" do
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

  describe "#consumer_info" do
    subject(:info) { redis_commands.consumer_info }

    context "when there are no consumers" do
      it { is_expected.to be_empty }
    end

    context "when there are consumers for our group" do
      let!(:consumer) { create_consumer }

      it "contains the consumer information" do
        is_expected.to have_key(consumer.name)
      end
    end

    context "when there are consumers for another group" do
      let!(:consumer) { create_consumer(group: "another_group") }

      subject(:other_info) { redis_commands.consumer_info("another_group") }

      it "returns nothing for our group" do
        expect(info).to be_empty
      end

      it "returns data for another group" do
        expect(other_info).to have_key(consumer.name)
      end
    end

    context "when a filter is provided" do
      let!(:consumer) { create_consumer }
      let!(:consumer_2) { create_consumer }

      subject(:info) { redis_commands.consumer_info(filter_for: [consumer.name]) }

      it "returns the filtered hash" do
        expect(info).to have_key(consumer.name)
        expect(info).not_to have_key(consumer_2.name)
      end
    end
  end

  describe "#claim_entry" do
    let!(:consumer) { create_consumer }
    let!(:entry) { add_to_stream }

    subject(:claimed_entry) { redis_commands.claim_entry(consumer.name, entry) }

    before { next_unread_entry("auto_dispatcher") }

    it "assigns the entry to the consumer" do
      expect(consumer_info_for(consumer.name)).to include("pending" => 0)

      expect(claimed_entry).to eq(entry)
      expect(consumer_info_for(consumer.name)).to include("pending" => 1)
    end
  end

  describe "#available_consumer_names" do
    subject(:consumer_names) { redis_commands.available_consumer_names }

    context "when there are available consumers" do
      let!(:consumer) { create_consumer }

      before { consumer.listen }

      it "returns a list including the consumer's name" do
        is_expected.to eq([consumer.name])
      end
    end

    context "when there are no available consumers" do
      it "returns an empty list" do
        is_expected.to be_empty
      end
    end
  end

  describe "#clear_available_consumers" do
    let!(:consumer) { create_consumer }

    subject(:deletion) { redis_commands.clear_available_consumers }

    before { consumer.listen }

    it "deletes the key" do
      expect(redis_commands.available_consumer_names).to include(consumer.name)
      expect(deletion).to eq(1)
      expect(redis_commands.available_consumer_names).to be_empty
    end
  end

  describe "#consumer_available?" do
    let(:consumer) { create_consumer }

    subject(:available) { redis_commands.consumer_available?(consumer.name) }

    context "when the consumer is available" do
      before { redis_commands.make_consumer_available(consumer.name) }

      it { is_expected.to be true }
    end

    context "when the consumer is not available" do
      it { is_expected.to be false }
    end
  end

  describe "#make_consumer_available" do
    let!(:consumer) { create_consumer }

    subject(:consumer_is_now_available) { redis_commands.make_consumer_available(consumer.name) }

    context "when the consumer is not available" do
      it "is added to the availability list" do
        expect(redis_commands.consumer_available?(consumer.name)).to be false

        consumer_is_now_available

        expect(redis_commands.consumer_available?(consumer.name)).to be true
        expect(redis_commands.available_consumer_names).to include(consumer.name)
      end
    end

    context "when the consumer is already available" do
      before { redis_commands.make_consumer_available(consumer.name) }

      it "does nothing" do
        expect(redis_commands.consumer_available?(consumer.name)).to be true
        expect(redis_commands.available_consumer_names.size).to eq(1)

        expect { consumer_is_now_available }.not_to raise_error

        expect(redis_commands.available_consumer_names.size).to eq(1)
        expect(redis_commands.consumer_available?(consumer.name)).to be true
        expect(redis_commands.available_consumer_names).to include(consumer.name)
      end
    end
  end

  describe "#make_consumer_unavailable" do
    let!(:consumer) { create_consumer }

    subject(:consumer_is_now_unavailable) { redis_commands.make_consumer_unavailable(consumer.name) }

    context "when the consumer is not available" do
      it "does nothing" do
        expect(redis_commands.consumer_available?(consumer.name)).to be false

        consumer_is_now_unavailable

        expect(redis_commands.consumer_available?(consumer.name)).to be false
        expect(redis_commands.available_consumer_names).not_to include(consumer.name)
      end
    end

    context "when the consumer is available" do
      before { redis_commands.make_consumer_available(consumer.name) }

      it "removes the consumer from the list" do
        expect(redis_commands.consumer_available?(consumer.name)).to be true
        expect(redis_commands.available_consumer_names.size).to eq(1)

        consumer_is_now_unavailable

        expect(redis_commands.available_consumer_names.size).to eq(0)
        expect(redis_commands.consumer_available?(consumer.name)).to be false
        expect(redis_commands.available_consumer_names).not_to include(consumer.name)
      end
    end
  end
end
