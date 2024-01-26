## [Unreleased]

## [1.0.2] - 2024-01-25
- Added support for multiple instances running under the same group name.
    - Useful for production environments that run an application via multiple processes (e.g. Rails)
- Exposed `entry`, `stream_name`, `group_name`, and `instance_id` methods to Channel events
- Added default `on_error` callbacks for Stream and Channel. All this does is log
- Increased pool size padding
- Added `RedisIPC::Stream::Commands#prune_consumers` that is called when a stream connects. This will remove any stale consumers
- Added `RedisIPC::Stream::Entry#pending?`

## [1.0.1] - 2024-01-14

- Adjusted ActiveSupport to support the active versions of Rails
- Added exception if `Channel.connect` is called multiple times
- Adjusted JSON parsing to parse keys as Symbol
- Improved logging and made it less difficult to look at
- Fixed an issue where a Stream would not pick back up where it was went it left off
- Added support for `return` in `Channel.event` blocks

## [1.0.0] - 2024-01-13

- Added `RedisIPC::Channel`
- Added `RedisIPC::Response`
- Added `RedisIPC::Channel#connected?` to indicate if a channel is connected
- Changed `RedisIPC::Stream#send_to_group` to return `RedisIPC::Response` and no longer raises when an entry times out. When an entry does time out, a rejected response will be returned

## [0.5.0] - 2024-01-10

- Initial release
