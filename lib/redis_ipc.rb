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

[:debug, :info, :warn, :error].each do |severity|
  define_method("#{severity}!") do |content = {}|
    RedisIPC.__log(severity, caller_locations(1, 1).first, content)
  end
end

module RedisIPC
  DEFAULTS = {
    host: ENV.fetch("REDIS_HOST", "localhost"),
    port: ENV.fetch("REDIS_PORT", 6379)
  }.freeze

  class Error < StandardError; end

  class TimeoutError < Error; end

  class << self
    attr_accessor :logger

    # Used internally by logging methods. Do not call manually
    def __log(severity, caller_data, content)
      return if logger.nil?

      if content.is_a?(Hash) && content[:error].is_a?(StandardError)
        e = content[:error]

        content[:error] = {
          message: e.message,
          backtrace: e.backtrace[0..20]
        }
      end

      caller_class = caller_data
        .path
        .sub("#{__dir__}/", "")
        .sub(".rb", "")
        .classify

      caller_method = caller_data.label.gsub("block in ", "")

      logger.send(severity, "#{caller_class}##{caller_method}:#{caller_data.lineno}") do
        if content.is_a?(Hash)
          JSON.pretty_generate(content).presence || ""
        else
          content || ""
        end
      end
    end
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
