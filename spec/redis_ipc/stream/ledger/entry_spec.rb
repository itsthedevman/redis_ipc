# frozen_string_literal: true

describe RedisIPC::Stream::Ledger::Entry do
  describe "#expired?" do
    context "when the entry is expired" do
      subject(:expired_entry) do
        described_class.new(expires_at: 5.seconds.ago, mailbox: "").expired?
      end

      it { is_expected.to be true }
    end

    context "when the entry is not expired" do
      subject(:not_expired_entry) do
        described_class.new(expires_at: 5.seconds.from_now, mailbox: "").expired?
      end

      it { is_expected.to be false }
    end
  end
end
