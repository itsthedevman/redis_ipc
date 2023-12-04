# frozen_string_literal: true

describe RedisIPC::Entry do
  subject(:entry) { described_class.new(group: "group", content: "content") }

  it "is valid" do
    expect { entry }.not_to raise_error
    expect(entry.group).to eq("group")
    expect(entry.content).to eq("content")
  end

  context "when no id is provided" do
    it "stores nil for the id" do
      expect(entry.id).to be_nil
    end
  end

  context "when an id is provided" do
    subject(:entry) { described_class.new(id: "id", group: "group", content: "content") }

    it "stores the id" do
      expect(entry.id).to eq("id")
    end
  end

  context "when no consumer is provided" do
    it "stores nil for the consumer" do
      expect(entry.consumer).to be_nil
    end
  end

  context "when a consumer is provided" do
    subject(:entry) { described_class.new(consumer: "consumer", group: "group", content: "content") }

    it "stores the consumer" do
      expect(entry.consumer).to eq("consumer")
    end
  end
end
