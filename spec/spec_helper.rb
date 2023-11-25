# frozen_string_literal: true

require "redis_ipc"
require "pry"

Dir.glob(File.expand_path("./spec/support/**/*.rb")).each { |m| require m }
RedisIPC.logger = Logger.new($stdout)

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end
end
