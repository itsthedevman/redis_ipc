# frozen_string_literal: true

module RedisIPC
  class Stream
    class Ledger
      #
      # A consumer that only consumes messages that are registered with the ledger
      #
      # @note It felt better to me to let Consumer be generic
      #
      class Consumer < Consumer
        def initialize(*, ledger:, **)
          super(*, **)
          @ledger = ledger
        end

        def check_for_entries
          entry = read_from_stream
          return if entry.nil?

          if group_name != entry.destination_group
            reject!(entry)
            return
          end

          # Entry received is in response to a previous request but failed to reply back in time.
          ledger_entry = @ledger[entry]

          # Sent to the group for processing by our consumers.
          # This entry is not in response to a previous request sent out by our sender
          is_a_request = ledger_entry.nil? && entry.status == "pending"

          # Response: Entry received in response to a previous request sent out from our sender.
          is_a_response = !ledger_entry.nil? && ["fulfilled", "rejected"].include?(entry.status)

          RedisIPC.logger&.debug {
            "'#{name}' - #{entry.id}:#{entry.redis_id} - Ledger? #{!ledger_entry.nil?} - Request? #{is_a_request} - Response? - #{is_a_response}"
          }

          if !is_a_request && !is_a_response
            reject!(entry)
            return
          end

          if ledger_entry
            ledger_entry.mailbox.put(entry.content)
            return
          end

          RedisIPC.logger&.debug { "'#{name}' - #{entry.id} before observing" }

          # TimerTask notifies the observers AFTER scheduling the next execution of the task
          # To ensures all observers have a chance to execute their code before the task is ran again
          # This also allows me to acknowledge and remove the message before this consumer attempts to read it again
          @task.instance_exec(entry) do |entry|
            observers.notify_observers { [Time.current, entry, nil] }
          end

          RedisIPC.logger&.debug { "'#{name}' - #{entry.id} acknowledged" }

          nil
        ensure
          acknowledge_and_remove(entry) if entry
        end
      end
    end
  end
end
