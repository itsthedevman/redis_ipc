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
    @redis ||= Redis.new(**RedisIPC::REDIS_DEFAULTS)
  end

  def create_stream
    return if redis.exists?(stream_name)

    redis.xgroup(:create, stream_name, group_name, "$", mkstream: true)
  end

  def destroy_stream
    redis.xgroup(:destroy, stream_name, group_name)
    redis.del(stream_name)
  end

  def wait_for_response!(consumer, type, ack: true)
    response = nil

    observer = consumer.add_observer do |time, result, exception|
      response = exception || result
      consumer.stop_listening
    end

    task = consumer.listen(type)

    while task.running?
      sleep(0.1)
    end

    consumer.delete_observer(observer)
    raise response if response.is_a?(Exception)

    redis.xack(stream_name, group_name, response[:message_id]) if ack
    response
  end
end
