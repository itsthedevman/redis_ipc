# frozen_string_literal: true

describe RedisIPC::Entry do
  subject(:entry) do
    described_class.new(destination_group: "destination_group", content: "content")
  end

  it "is valid" do
    expect { entry }.not_to raise_error
    expect(entry.destination_group).to eq("destination_group")
    expect(entry.content).to eq("content")
  end

  context "when no id is provided" do
    it "stores nil for the id" do
      expect(entry.id).to be_nil
    end
  end

  context "when an id is provided" do
    subject(:entry) { described_class.new(id: "id", destination_group: "destination_group", content: "content") }

    it "stores the id" do
      expect(entry.id).to eq("id")
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
      subject(:entry) { described_class.new(id: "id", destination_group: "destination_group", content: "content") }

      it "stores the id" do
        expect(hash).to have_key(:id)
        expect(hash[:id]).to eq("id")
      end
    end
  end
end
