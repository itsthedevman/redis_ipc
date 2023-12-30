# frozen_string_literal: true

describe RedisIPC::Stream::Dispatcher do
  include_context "stream"

  let(:dispatcher) { create_dispatcher }

  describe "#check_for_entries" do
    let!(:consumer) do
      consumer = create_consumer
      consumer.redis.make_consumer_available(consumer)
      consumer
    end

    context "when an entry is for the group" do
      before do
        add_to_stream(example_entry.with(destination_group: group_name))
      end

      it "assigns it to a consumer" do
        expect(entries_size).to eq(1)
        expect(consumer_info_for(dispatcher)&.pending).to eq(0)
        expect(consumer_info_for(consumer)&.pending).to eq(0)

        dispatcher.check_for_entries

        expect(entries_size).to eq(1)
        expect(consumer_info_for(dispatcher)&.pending).to eq(0)
        expect(consumer_info_for(consumer)&.pending).to eq(1)
      end
    end

    context "when the entry is not for the group" do
      let!(:other_consumer) do
        consumer = create_consumer(group: "other_group")
        consumer.redis.make_consumer_available(consumer)
        consumer
      end

      let!(:other_dispatcher) { create_dispatcher(group: "other_group") }

      before do
        add_to_stream(example_entry.with(destination_group: "other_group"))
      end

      it "ignores it" do
        expect(entries_size).to eq(1)
        expect(consumer_info_for(dispatcher)&.pending).to eq(0)
        expect(consumer_info_for(consumer)&.pending).to eq(0)

        dispatcher.check_for_entries

        expect(entries_size).to eq(1)
        expect(consumer_info_for(dispatcher)&.pending).to eq(0)
        expect(consumer_info_for(consumer)&.pending).to eq(0)
      end
    end
  end

  describe "#find_load_balanced_consumer" do
    let!(:consumers) do
      5.times.map do |i|
        consumer = create_consumer
        consumer.redis.make_consumer_available(consumer)
        consumer
      end
    end

    subject(:balanced_consumer_name) { dispatcher.send(:find_load_balanced_consumer)&.name }

    context "when there are no pending entries" do
      let!(:consumer_names) { consumers.map(&:name) }

      it "will pick the first consumer created" do
        # This is backwards since array.include?
        expect(consumer_names).to include(balanced_consumer_name)
      end
    end

    context "when some consumers have pending entries and some don't" do
      let(:consumer_sampler) { consumers.sample(3) } # Leaves 2 consumers

      before do
        consumer_sampler.each do |consumer|
          send_to_consumer(consumer, content: Faker::String.random)
        end
      end

      it "will pick a consumer with no entries" do
        (free_consumer_1, free_consumer_2) = consumers - consumer_sampler
        expect([free_consumer_1.name, free_consumer_2.name]).to include(balanced_consumer_name)
      end
    end

    context "when all consumers have entries, but some have less than others" do
      before do
        consumers.shuffle.each_with_index do |consumer, index|
          # Using the index to ensure a the proper ordering to check against
          (index + 1).times do
            send_to_consumer(consumer, content: Faker::String.random)
          end
        end
      end

      it "will pick a consumer with the least amount of pending entries" do
        consumer = consumer_info.values.min_by(&:pending)

        expect(balanced_consumer_name).to eq(consumer.name)
      end
    end

    context "when all consumers have equal pending entries" do
      before do
        consumers.shuffle.each do |consumer|
          sleep(rand / 1000) # We're working with the milliseconds, this doesn't need to delay very long

          send_to_consumer(consumer, content: Faker::String.random)
        end
      end

      it "will pick an active consumer who has idled the longest" do
        # I know all of the consumers have pending entries so I can skip that
        consumer = consumer_info.values.min { |a, b| b.idle <=> a.idle }

        expect(balanced_consumer_name).to eq(consumer.name)
      end
    end
  end
end
