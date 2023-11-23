# frozen_string_literal: true

describe RedisIPC::Stream do
  let!(:stream_name) { "example_stream" }
  let!(:group_name) { "example_group" }

  subject(:stream) do
    described_class.new(stream_name, group: group_name)
  end

  context "when the Stream is initialized" do
    it "connects to redis and creates the stream" do
      expect(stream.redis_pool).not_to be_blank

      stream.redis_pool.with do |redis|
        expect(redis.exists?(stream_name))
      end
    end
  end

  context "when content is sent over the Stream" do
    it "returns the response"
  end
end
