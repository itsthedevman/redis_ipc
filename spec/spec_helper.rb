# frozen_string_literal: true

require "redis_ipc"
require "pry"
require "faker"

Dir.glob(File.expand_path("./spec/support/**/*.rb")).each { |m| require m }

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"
  config.tty = true

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end
end
