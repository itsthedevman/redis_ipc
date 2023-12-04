# frozen_string_literal: true

require "active_support"
require "active_support/core_ext/array/access"
require "active_support/core_ext/class/attribute"
require "active_support/core_ext/enumerable"
require "active_support/core_ext/hash/indifferent_access"
require "active_support/core_ext/module/delegation"
require "active_support/core_ext/object/blank"
require "active_support/core_ext/string/inflections"
require "concurrent"
require "connection_pool"
require "json"
require "redis"


module RedisIPC
  DEFAULTS = {
    host: ENV.fetch("REDIS_HOST", "localhost"),
    port: ENV.fetch("REDIS_PORT", 6379)
  }.freeze

  class Error < StandardError; end

  class TimeoutError < Error; end

  class ConnectionError < Error; end

  class ConfigurationError < Error; end

  class << self
    attr_accessor :logger
  end
end

[
  "consumer",
  "dispatcher",
  "entry",
  "sender",
  "stream",
  "version"
].each { |m| require_relative "./redis_ipc/#{m}" }
