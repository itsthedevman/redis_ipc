# frozen_string_literal: true

describe RedisIPC::Entry do
  subject(:entry) do
    described_class.new(source_group: "source_group", destination_group: "destination_group", content: "content")
  end

  it "is valid" do
    expect { entry }.not_to raise_error
    expect(entry.source_group).to eq("source_group")
    expect(entry.destination_group).to eq("destination_group")
    expect(entry.content).to eq("content")
  end

  context "when no id is provided" do
    it "stores nil for the id" do
      expect(entry.id).to be_nil
    end
  end

  context "when an id is provided" do
    subject(:entry) do
      described_class.new(
        id: "id",
        return_to_consumer: "consumer",
        source_group: "source_group",
        destination_group: "destination_group",
        content: "content"
      )
    end

    it "stores the id" do
      expect(entry.id).to eq("id")
    end
  end

  context "when no consumer is provided" do
    it "stores nil for the consumer" do
      expect(entry.return_to_consumer).to be_nil
    end
  end

  context "when a consumer is provided" do
    subject(:entry) do
      described_class.new(
        return_to_consumer: "consumer",
        source_group: "source_group",
        destination_group: "destination_group",
        content: "content"
      )
    end

    it "stores the consumer" do
      expect(entry.return_to_consumer).to eq("consumer")
    end
  end

  describe "#to_h" do
    subject(:hash) { entry.to_h }

    context "when no id is provided" do
      it "stores nil for the id" do
        expect(hash).not_to have_key(:id)
      end
    end

    context "when an id is provided" do
      subject(:entry) do
        described_class.new(
          id: "id",
          return_to_consumer: "consumer",
          source_group: "source_group",
          destination_group: "destination_group",
          content: "content"
        )
      end

      it "stores the id" do
        expect(hash).to have_key(:id)
        expect(hash[:id]).to eq("id")
      end
    end

    context "when no consumer is provided" do
      it "stores nil for the consumer" do
        expect(hash).not_to have_key(:return_to_consumer)
      end
    end

    context "when a consumer is provided" do
      subject(:entry) do
        described_class.new(
          return_to_consumer: "consumer",
          source_group: "source_group",
          destination_group: "destination_group",
          content: "content"
        )
      end

      it "stores the consumer" do
        expect(hash).to have_key(:return_to_consumer)
        expect(hash[:return_to_consumer]).to eq("consumer")
      end
    end
  end

  describe "#for_response" do
    subject(:response) { entry.for_response }

    it "returns an instance of Entry" do
      expect(response).to be_instance_of(described_class)
    end

    it "swaps source and destination groups" do
      expect(response.source_group).to eq(entry.destination_group)
      expect(response.destination_group).to eq(entry.source_group)
    end

    context "when content is not provided" do
      it "sets the content to nil" do
        expect(response.content).to eq(nil)
      end
    end

    context "when content is provided" do
      subject(:response) { entry.for_response(content: "Hello") }

      it "replaces the content" do
        expect(response.content).to eq("Hello")
      end
    end
  end

  describe "#response?" do
    context "when the ID is set" do
      subject(:id_entry) { entry.with(id: "id") }

      it { expect(id_entry.response?).to be true }
    end

    context "when the ID is not set" do
      it { expect(entry.response?).to be false }
    end
  end
end
