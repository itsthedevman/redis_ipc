# frozen_string_literal: true

source "https://rubygems.org"

# Specify your gem's dependencies in redis_ipc.gemspec
gemspec

group :development, :test do
  gem "faker"
  gem "rspec", "~> 3.0"
  gem "rubocop", "~> 1.21"
  gem "standardrb", "~> 1.0"
  gem "rubocop-performance", "~> 1.19"
  gem "rubocop-rspec", "~> 2.24"
  gem "rubocop-rails", "~> 2.21", require: false
  gem "pry"
  gem "benchmark-ips"
end

group :development, :documentation do
  gem "yard"
  gem "kramdown"
end
