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

  def send_and_wait!(consumer, content:, ack: false)
    entry = RedisIPC::Entry.new(group: group_name, content: content)
    redis.xadd(stream_name, entry.to_h)
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

    consumer.acknowledge(response.message_id) if ack
    response
  end

  def claim_message(consumer, message)
    redis.xclaim(stream_name, group_name, consumer.name, 0, message.id)
  end
end
