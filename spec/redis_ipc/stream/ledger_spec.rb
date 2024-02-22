# frozen_string_literal: true

describe RedisIPC::Stream::Ledger do
  include_context "stream"

  subject(:ledger) { described_class.new }

  describe "#add" do
    subject!(:stored_entry) { ledger.add(example_entry) }

    context "when the entry is added to the ledger" do
      it "stores the entry and returns the MVar (mailbox)" do
        mailbox = ledger.remove(example_entry)
        expect(mailbox).to be_an_instance_of(Concurrent::MVar)
      end
    end
  end
end
