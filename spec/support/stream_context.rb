# frozen_string_literal: true

RSpec.shared_context("stream") do
  let!(:stream_name) { "example_stream" }
  let!(:group_name) { "example_group" }
  let!(:logger) { Logger.new($stdout) }

  let!(:redis_commands) do
    # NOTE: Reset here is deleting the group
    RedisIPC::Stream::Commands.new(stream_name, group_name, logger: logger, reset: true)
  end

  let(:redis_pool) { redis_commands.redis_pool }
  let(:redis) { redis_pool.checkout }

  let(:example_entry) do
    RedisIPC::Stream::Entry.new(
      source_group: group_name,
      destination_group: "other_example_group",
      content: "Hello"
    )
  end

  after do
    redis_commands.delete_stream

    # Forcing the checkin will silence the error
    redis_pool.checkin(force: true)
  end

  delegate :create_group, :entries_size, :consumer_info, :claim_entry,
    :next_unread_entry, :next_pending_entry,
    to: :redis_commands

  def create_consumer(name = nil, group: nil, consumer_class: RedisIPC::Stream::Consumer, **)
    # stream and group for a consumer come from the redis connection
    redis =
      if group
        RedisIPC::Stream::Commands.new(stream_name, group, logger: logger)
      else
        redis_commands
      end

    name ||= "#{redis.stream_name}:#{redis.group_name}-#{consumer_class.name.demodulize.downcase}:#{SecureRandom.uuid.delete("-")[0..5]}"

    consumer_class.new(name, redis: redis, **)
  end

  def add_to_stream(entry = example_entry, redis: redis_commands)
    redis_id = redis.add_to_stream(entry)
    entry.with(redis_id: redis_id)
  end

  def consumer_info_for(consumer_name)
    consumer_info[consumer_name]
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
