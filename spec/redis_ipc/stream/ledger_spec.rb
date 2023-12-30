# frozen_string_literal: true

describe RedisIPC::Stream::Ledger do
  include_context "stream"

  subject(:ledger) { described_class.new(entry_timeout: 3, cleanup_interval: 1) }

  describe "#add" do
    context "when the id is not in the ledger" do
      it "creates a new Ledger::Entry and adds it" do
        ledger.add(example_entry)

        entry = ledger[example_entry]
        expect(entry).to be_an_instance_of(described_class::Entry)
        expect(entry.to_h).to include(
          expires_at: be_between(Time.current, 5.seconds.from_now)
        )
      end
    end

    context "when the id is already in the ledger" do
      it "raises an exception" do
        ledger.add(example_entry)

        expect { ledger.add(example_entry) }.to raise_error(ArgumentError, "#{example_entry.id} is already in the ledger")
      end
    end
  end
end
