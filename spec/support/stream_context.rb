# frozen_string_literal: true

RSpec.shared_context("stream") do
  let!(:stream_name) { "example_stream" }
  let!(:group_name) { "example_group" }

  before do
    create_stream
  end

  after do
    destroy_stream
  end

  def redis
    @redis ||= Redis.new(**RedisIPC::DEFAULTS)
  end

  def create_stream
    return if redis.exists?(stream_name)

    redis.xgroup(:create, stream_name, group_name, "$", mkstream: true)
  end

  def destroy_stream
    redis.xgroup(:destroy, stream_name, group_name)
    redis.del(stream_name)
  end

  def add_to_stream(consumer = nil, content:)
    entry = RedisIPC::Entry.new(consumer: consumer, group: group_name, content: content)
    redis.xadd(stream_name, entry.to_h)
  end

  def send_to(consumer, dispatcher = nil, content:)
    id = add_to_stream(content: content)

    redis.xreadgroup(group_name, dispatcher&.name || "auto_dispatcher", stream_name, ">", count: 1)
    assign_message_to(consumer, id)

    id
  end

  def send_and_wait!(consumer, content:, ack: false)
    add_entry_to_stream(consumer, content: content)
    wait_for_response!(consumer, ack: ack)
  end

  def wait_for_response!(consumer, ack: true)
    response = nil

    observer = consumer.add_observer do |time, result, exception|
      response = exception || result
      consumer.stop_listening
    end

    task = consumer.listen

    while task.running?
      sleep(0.1)
    end

    consumer.delete_observer(observer)
    return if response.nil?
    raise response if response.is_a?(Exception)

    consumer.acknowledge(response.id) if ack
    response
  end

  def assign_message_to(consumer, message_or_id)
    redis.xclaim(
      stream_name,
      group_name,
      consumer.name,
      0,
      message_or_id.is_a?(RedisIPC::Entry) ? message_or_id.id : message_or_id
    )
  end
end
