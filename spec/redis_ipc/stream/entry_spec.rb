# frozen_string_literal: true

describe RedisIPC::Stream::Entry do
  subject(:entry) do
    described_class.new(
      instance_id: "",
      source_group: "source_group",
      destination_group: "destination_group",
      content: "content"
    )
  end

  it "is valid" do
    expect { entry }.not_to raise_error
    expect(entry.destination_group).to eq("destination_group")
    expect(entry.content).to eq("content")
  end

  context "when no id is provided" do
    it "generates an id" do
      expect(entry.id).not_to be_nil
    end
  end

  context "when an id is provided" do
    subject(:entry) do
      described_class.new(
        id: "id",
        instance_id: "",
        source_group: "source_group",
        destination_group: "destination_group",
        content: "content"
      )
    end

    it "stores the id" do
      expect(entry.id).to eq("id")
    end
  end

  describe "#to_h" do
    subject(:hash) { entry.to_h }

    subject(:entry) do
      described_class.new(
        id: "id",
        instance_id: "id",
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
end
