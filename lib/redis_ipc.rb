# frozen_string_literal: true

require "active_support/all"
require "concurrent"
require "connection_pool"
require "redis"

module RedisIPC
  class Error < StandardError; end
end

[
  "consumer",
  "stream",
  "version"
].each { |m| require_relative "./redis_ipc/#{m}" }

puts "Hello"
