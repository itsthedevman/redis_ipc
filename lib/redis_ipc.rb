# frozen_string_literal: true

require "active_support"
require "active_support/core_ext/class/attribute"
require "active_support/core_ext/object/blank"
require "active_support/core_ext/module/delegation"
require "concurrent"
require "connection_pool"
require "redis"

module RedisIPC
  class Error < StandardError; end
  class TimeoutError < Error; end
end

[
  "consumer",
  "dispatcher",
  "response_observer",
  "stream",
  "version"
].each { |m| require_relative "./redis_ipc/#{m}" }
