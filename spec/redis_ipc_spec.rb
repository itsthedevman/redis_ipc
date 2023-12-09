# frozen_string_literal: true

describe RedisIPC do
  describe ".ledger_key" do
    it "returns the ledger key" do
      expect(RedisIPC.ledger_key("stream_name", "id")).to eq("stream_name:ledger:id")
    end
  end
end
