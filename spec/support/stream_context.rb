# frozen_string_literal: true

RSpec.shared_context("stream") do
  let!(:stream_name) { "example_stream" }
  let!(:group_name) { "example_group" }

  before do
    destroy_stream
    create_stream
  end

  after do
    destroy_stream
  end

  def redis
    @redis ||= Redis.new(**RedisIPC::Stream::REDIS_DEFAULTS)
  end

  def consumer_stats
    redis.xinfo(:consumers, stream_name, group_name)
  end

  def consumer_only_stats(consumer_names)
    consumer_stats.select { |c| consumer_names.include?(c["name"]) }
  end

  def consumer_stats_for(consumer)
    consumer_stats.find { |c| c["name"] == consumer.name }
  end

  def create_stream
    return if redis.exists?(stream_name)

    redis.xgroup(:create, stream_name, group_name, "$", mkstream: true)
  end

  def destroy_stream
    redis.xgroup(:destroy, stream_name, group_name)
    redis.del(stream_name)
  rescue
    nil
  end

  def add_to_stream(entry)
    redis.xadd(stream_name, entry.to_h)
  end

  def send_and_delegate_to_consumer(consumer, dispatcher = nil, content:)
    entry = RedisIPC::Stream::Entry.new(
      content: content,
      source_group: group_name,
      destination_group: consumer.group_name
    )

    id = add_to_stream(entry)
    redis.xreadgroup(group_name, dispatcher&.name || "auto_dispatcher", stream_name, ">", count: 1)
    redis.xclaim(stream_name, group_name, consumer.name, 0, id)

    [id, entry]
  end

  def wait_for_response!(consumer, ack: true)
    response = nil

    observer = consumer.add_observer do |_, (id, result), exception|
      response = exception || result
      consumer.stop_listening
    end

    task = consumer.listen

    count = 0
    while task.running? && count < 20 # 2 seconds
      sleep(0.1)
      count += 1
    end

    # Cleanup
    consumer.stop_listening if task.running?
    consumer.delete_observer(observer)

    return if response.nil?
    raise response if response.is_a?(Exception)

    consumer.acknowledge_and_remove(response.id) if ack
    response
  end
end
