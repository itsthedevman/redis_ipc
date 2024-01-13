## [Unreleased]

## [1.0.0] - 2024-01-13

- Added `RedisIPC::Channel`
- Added `RedisIPC::Response`
- Added `RedisIPC::Channel#connected?` to indicate if a channel is connected
- Changed `RedisIPC::Stream#send_to_group` to return `RedisIPC::Response` and no longer raises when an entry times out. When an entry does time out, a rejected response will be returned

## [0.5.0] - 2024-01-10

- Initial release
