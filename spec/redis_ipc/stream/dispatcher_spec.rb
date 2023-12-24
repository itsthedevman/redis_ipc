# frozen_string_literal: true

describe RedisIPC::Stream::Dispatcher do
  include_context "stream"

  let!(:consumers) do
    5.times.map do |i|
      RedisIPC::Stream::Consumer.new("test_consumer_#{i}", stream: stream_name, group: group_name)
    end
  end

  let!(:consumer_names) { consumers.map(&:name) }

  subject(:dispatcher) do
    described_class.new("test_dispatcher", consumer_names, stream: stream_name, group: group_name)
  end

  it "is valid" do
    expect { dispatcher }.not_to raise_error
    expect(dispatcher.stream_name).to eq(stream_name)
    expect(dispatcher.group_name).to eq(group_name)
    expect(dispatcher.name).to eq("test_dispatcher")
  end

  context "when created without a stream name" do
    subject(:dispatcher) { described_class.new("test_dispatcher", [], stream: "", group: group_name) }

    it "raises an exception with a different class name to Consumer" do
      expect { dispatcher }.to raise_error(
        ArgumentError, "Dispatcher test_dispatcher was created without a stream name"
      )
    end
  end

  describe "#listen" do
    context "when data is first received by the group" do
      it "reads in the data as an Entry and assigns it to the least busy consumer" do
        entry = RedisIPC::Stream::Entry.new(
          content: "Hello",
          source_group: group_name,
          destination_group: group_name
        )

        add_to_stream(entry)
        expect(consumer_stats).to be_empty
        expect(redis.xlen(stream_name)).to eq(1)

        dispatcher.listen
        sleep(rand)
        dispatcher.stop_listening

        expect(redis.xlen(stream_name)).to eq(1)
        expect(consumer_stats).to include(hash_including("name" => "test_consumer_0", "pending" => 1))
      end
    end
  end

  describe "#find_load_balanced_consumer" do
    subject(:balanced_consumer_name) { dispatcher.send(:find_load_balanced_consumer) }

    before do
      # Remove the default functionality so we can use the dispatcher to listen for entries
      dispatcher.delete_observer(dispatcher)
    end

    # zero pending entries
    # 2 consumers have entries, 1 does not
    # All consumers have entries, 2 have less than the other
    # All 3 consumers have the same number of entries
    context "when there are no pending entries" do
      it "will pick the first consumer created" do
        expect(balanced_consumer_name).to eq("test_consumer_0")
      end
    end

    context "when some consumers have pending entries and some don't" do
      let(:consumer_sampler) { consumers.sample(3) } # Leaves 2 consumers

      before do
        consumer_sampler.each do |consumer|
          send_and_delegate_to_consumer(consumer, dispatcher, content: Faker::String.random)
        end
      end

      it "will pick a consumer with no entries" do
        (free_consumer_1, free_consumer_2) = consumers - consumer_sampler
        expect(balanced_consumer_name).to eq(free_consumer_1.name) || eq(free_consumer_2.name)
      end
    end

    context "when all consumers have entries, but some have less than others" do
      before do
        consumers.shuffle.each_with_index do |consumer, index|
          # Using the index to ensure a the proper ordering to check against
          (index + 1).times do
            send_and_delegate_to_consumer(consumer, dispatcher, content: Faker::String.random)
          end
        end
      end

      it "will pick a consumer with the least amount of pending entries" do
        consumer = consumer_only_stats(consumer_names).min_by { |c| c["pending"] }

        expect(balanced_consumer_name).to eq(consumer["name"])
      end
    end

    context "when all consumers have equal pending entries" do
      before do
        consumers.shuffle.each do |consumer|
          sleep(rand / 1000) # We're working with the milliseconds, this doesn't need to delay very long

          send_and_delegate_to_consumer(consumer, dispatcher, content: Faker::String.random)
        end
      end

      it "will pick an active consumer who has idled the longest" do
        # I know all of the consumers have pending entries so I can skip that
        consumer = consumer_only_stats(consumer_names).min { |a, b| b["idle"] <=> a["idle"] }

        expect(balanced_consumer_name).to eq(consumer["name"])
      end
    end
  end

  describe "#accept?" do
    # A request is a entry that was sent to the group for processing by our consumers.
    # This entry is not in response to a previous request sent out by us
    context "when the inbound entry is a Request" do

    end

    # Entry received is in response to a previous request sent out from our sender.
    context "when the inbound entry is a Response"

    # Entry received is in response to a previous request but failed to reply back in time.
    context "when the inbound entry is expired"
  end
end
