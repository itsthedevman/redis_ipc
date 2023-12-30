# frozen_string_literal: true

RSpec.shared_context("stream") do
  let!(:stream_name) { "example_stream" }
  let!(:group_name) { "example_group" }
  let(:logger) { Logger.new($stdout) }

  let!(:redis_commands_opts) { {logger: logger} } # Use if logging is needed
  let!(:redis_commands) do
    RedisIPC::Stream::Commands.new(stream_name, group_name, **redis_commands_opts)
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
    redis_commands.destroy_group
    redis_commands.create_group

    # Do not delete the stream/group here as that this can run AFTER consumers are created
    # Which causes issues when they go to check for entries

    # Tracking which groups are created to avoid cleaning up data
    @groups = Concurrent::Map.new
    @groups[redis_commands.group_name] = redis_commands
  end

  after do
    redis_commands.destroy_group
    redis_commands.delete_stream

    # Forcing the checkin will silence the error
    redis_pool.checkin(force: true)
  end

  delegate :create_group, :entries_size, :consumer_info, :claim_entry,
    :next_unread_entry, :next_pending_entry,
    to: :redis_commands

  def create_consumer(name = nil, group: nil, consumer_class: RedisIPC::Stream::Consumer, **)
    group ||= group_name
    name ||= "#{group}_#{consumer_class.name.demodulize.downcase}_#{Time.now.to_f}"

    redis = @groups[group] ||= RedisIPC::Stream::Commands.new(stream_name, group, **redis_commands_opts)
    redis.create_group # Since this command can create new groups, this needs to be here

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
