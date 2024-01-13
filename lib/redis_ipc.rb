# frozen_string_literal: true

require "active_support"
require "active_support/core_ext/array/access"
require "active_support/core_ext/class/attribute"
require "active_support/core_ext/enumerable"
require "active_support/core_ext/hash/indifferent_access"
require "active_support/core_ext/kernel/reporting"
require "active_support/core_ext/module/delegation"
require "active_support/core_ext/numeric/time"
require "active_support/core_ext/object/blank"
require "active_support/core_ext/string/inflections"
require "concurrent"
require "connection_pool"
require "json"
require "redis"

module RedisIPC
  class Error < StandardError; end

  class TimeoutError < Error; end

  class ConnectionError < Error; end

  class ConfigurationError < Error; end
end

[
  "channel",
  "response",
  "stream",
  "stream/commands",
  "stream/consumer",
  "stream/dispatcher",
  "stream/entry",
  "stream/ledger",
  "stream/ledger/consumer",
  "version"
].each { |m| require_relative "./redis_ipc/#{m}" }
