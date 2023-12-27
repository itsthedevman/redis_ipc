# frozen_string_literal: true

RSpec.shared_context("stream") do
  let!(:stream_name) { "example_stream" }
  let!(:group_name) { "example_group" }
  let!(:logger) { Logger.new($stdout) }

  let!(:redis_commands) do
    RedisIPC::Stream::Commands.new(stream_name, group_name, logger: logger)
  end

  let(:redis_pool) { redis_commands.redis_pool }
  let(:redis) { redis_pool.checkout }

  let(:example_entry) do
    RedisIPC::Stream::Entry.new(
      source_group: group_name,
      destination_group: "other_example_group",
      content: Faker::String.random
    )
  end

  before do
    # Tracking which groups are created to avoid cleaning up data
    @groups = Concurrent::Map.new
    @groups[redis_commands.group_name] = redis_commands
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
    group ||= group_name
    name ||= "#{consumer_class.name.demodulize.downcase}_#{SecureRandom.uuid.delete("-")[0..5]}"

    redis = @groups[group] ||= RedisIPC::Stream::Commands.new(stream_name, group, logger: logger)
    consumer_class.new(name, redis: redis, **)
  end

  def create_dispatcher(name = nil, group: nil, **)
    create_consumer(name, group: group, consumer_class: RedisIPC::Stream::Dispatcher, **)
  end

  def add_to_stream(entry = example_entry, redis: redis_commands)
    redis.add_to_stream(entry)
  end

  def consumer_info_for(consumer)
    consumer_info[consumer.name]
  end

  def send_to_consumer(consumer, content:)
    entry = RedisIPC::Stream::Entry.new(
      content: content,
      source_group: group_name,
      destination_group: consumer.group_name
    )

    add_to_stream(entry)
    next_unread_entry(consumer)
  end
end
