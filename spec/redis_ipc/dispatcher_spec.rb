# frozen_string_literal: true

describe RedisIPC::Dispatcher do
  include_context "stream"

  let!(:consumers) do
    5.times.map do |i|
      RedisIPC::Consumer.new("test_consumer_#{i}", stream: stream_name, group: group_name)
    end
  end

  let!(:consumer_names) { consumers.map(&:name) }

  subject(:dispatcher) do
    described_class.new("test_dispatcher", stream: stream_name, group: group_name, consumer_names: consumer_names)
  end

  it "is valid" do
    expect { dispatcher }.not_to raise_error
    expect(dispatcher.stream_name).to eq(stream_name)
    expect(dispatcher.group_name).to eq(group_name)
    expect(dispatcher.name).to eq("test_dispatcher")
  end

  describe "#on_message" do
    context "when a message arrives" do
      it "forwards it to a consumer" do
        dispatcher.listen
        sleep(9999999)
      end
    end
  end
end
