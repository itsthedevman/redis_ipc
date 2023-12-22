# frozen_string_literal: true

RSpec.shared_context("stream") do
  let!(:stream_name) { "example_stream" }
  let!(:group_name) { "example_group" }

  let(:redis_commands) { RedisIPC::Stream::Commands.new(stream_name, group_name) }
  let(:redis_pool) { redis_commands.redis_pool }
  let(:redis) { redis_pool.checkout }

  let(:example_entry) do
    RedisIPC::Stream::Entry.new(
      source_group: group_name,
      destination_group: "other_example_group",
      content: "Hello"
    )
  end

  before do
    destroy_stream
    create_group
  end

  after do
    destroy_stream

    # Rechecks in the redis connection if one is checked out
    redis_pool.checkin if defined?(:redis)
  end

  delegate :create_group, :entries_size, :consumer_info, :claim_entry,
    :next_unread_entry, :next_pending_entry,
    to: :redis_commands

  def add_to_stream(entry = example_entry)
    redis_id = redis_commands.add_to_stream(entry)
    entry.with(redis_id: redis_id)
  end

  def consumer_info_for(*)
    consumer_info.slice(*)
  end

  def destroy_stream
    redis.xgroup(:destroy, stream_name, group_name)
    redis.del(stream_name)
  rescue
    nil
  end

  def send_and_delegate_to_consumer(consumer, dispatcher = nil, content:)
    entry = RedisIPC::Stream::Entry.new(
      content: content,
      source_group: group_name,
      destination_group: consumer.group_name
    )

    id = add_to_stream(entry)
    entry = entry.with(redis_id: id)

    redis_commands.read_from_stream(dispatcher&.name || "auto_dispatcher", ">")
    claim_entry(consumer.name, entry)

    entry
  end
end
