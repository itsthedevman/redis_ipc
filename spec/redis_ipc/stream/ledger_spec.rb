# frozen_string_literal: true

describe RedisIPC::Stream::Ledger do
  subject(:ledger) { described_class.new(entry_timeout: 3, cleanup_interval: 1) }

  describe "#add" do
    context "when the id is not in the ledger" do
      it "creates a new Ledger::Entry and adds it" do
        ledger.add("foo", "consumer_name")

        entry = ledger["foo"]
        expect(entry).to be_an_instance_of(described_class::Entry)
        expect(entry.to_h).to include(
          expires_at: be_between(Time.current, 5.seconds.from_now),
          dispatch_to_consumer: "consumer_name"
        )
      end
    end

    context "when the id is already in the ledger" do
      it "raises an exception" do
        ledger.add("foo", "consumer_name")

        expect { ledger.add("foo", "another_consumer") }.to raise_error(ArgumentError, "foo is already in the ledger")
      end
    end
  end

  describe "#expired?" do
    subject { ledger.expired?("foo") }

    context "when the key is in the ledger" do
      before { ledger.add("foo", "") }

      it { is_expected.to be false }
    end

    context "when the key is not in the ledger" do
      it { is_expected.to be true }
    end
  end
end
