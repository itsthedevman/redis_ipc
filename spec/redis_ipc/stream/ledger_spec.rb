# frozen_string_literal: true

describe RedisIPC::Stream::Ledger do
  include_context "stream"

  subject(:ledger) { described_class.new(entry_timeout: 0.2, cleanup_interval: 0.1) }

  describe "#store_entry" do
    subject(:store_entry) { ledger.store_entry(example_entry) }

    before { store_entry }

    context "when the id is not in the ledger" do
      it "creates a new Ledger::Entry and adds it" do
        entry = ledger[example_entry.id]
        expect(entry).to be_an_instance_of(described_class::Entry)
        expect(entry.to_h).to include(
          expires_at: be_between(Time.current, 5.seconds.from_now)
        )
      end
    end

    context "when the id is already in the ledger" do
      it "raises an exception" do
        expect { ledger.store_entry(example_entry) }.to raise_error(ArgumentError, "#{example_entry.id} is already in the ledger")
      end
    end
  end

  context "when there are expired entries in the ledger" do
    before { ledger.store_entry(example_entry) }

    it "deletes them" do
      expect(ledger[example_entry.id]).not_to be_nil
      sleep(0.3)
      expect(ledger[example_entry.id]).to be_nil
    end
  end

  describe "#fetch_entry" do
    subject(:fetched_entry) { ledger.fetch_entry(example_entry) }

    context "when the entry is in the ledger" do
      before { ledger.store_entry(example_entry) }

      it "returns the ledger entry" do
        expect(fetched_entry).to be_an_instance_of(described_class::Entry)
      end
    end

    context "when the entry is not in the ledger" do
      it "returns nil" do
        expect(fetched_entry).to be_nil
      end
    end
  end

  describe "#delete_entry" do
    subject(:deleted_entry) { ledger.delete_entry(example_entry) }

    context "when the entry is in the ledger" do
      before { ledger.store_entry(example_entry) }

      it "removes the entry" do
        expect(ledger.size).to eq(1)

        expect(deleted_entry).not_to be_nil
        expect(ledger.size).to eq(0)
      end
    end

    context "when the entry is not in the ledger" do
      it "does nothing" do
        expect(ledger.size).to eq(0)

        expect(deleted_entry).to be_nil
        expect(ledger.size).to eq(0)
      end
    end
  end
end
