# frozen_string_literal: true

require_relative "lib/redis_ipc/version"

Gem::Specification.new do |spec|
  spec.name = "redis_ipc"
  spec.version = RedisIPC::VERSION
  spec.authors = ["Bryan"]
  spec.email = ["bryan@itsthedevman.com"]

  spec.summary = "Redis IPC"
  spec.required_ruby_version = ">= 3.0.0"

  spec.metadata["source_code_uri"] = "https://github.com/itsthedevman/redis_ipc_rb"
  spec.metadata["changelog_uri"] = "https://github.com/itsthedevman/redis_ipc_rb/CHANGELOG.md"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(__dir__) do
    `git ls-files -z`.split("\x0").reject do |f|
      (File.expand_path(f) == __FILE__) ||
        f.start_with?(*%w[bin/ test/ spec/ features/ .git .circleci appveyor Gemfile])
    end
  end

  spec.require_paths = ["lib"]

  #####################################
  # Dependencies
  #
  spec.add_dependency "activesupport", "~> 7.1"
  spec.add_dependency "concurrent-ruby", "~> 1.2"
  spec.add_dependency "concurrent-ruby-ext", "~> 1.2"
  spec.add_dependency "connection_pool", "~> 2.4"
  spec.add_dependency "rake", "~> 13.0"
  spec.add_dependency "redis", "~> 5.0"
end
