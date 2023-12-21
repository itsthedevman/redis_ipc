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
          return if group_name != entry.destination_group

          ledger_entry = @ledger[entry]
          is_a_request = ledger_entry.nil? && entry.status == "pending"
          is_a_response = !ledger_entry.nil? && ["fulfilled", "rejected"].include?(entry.status)

          if is_a_request
            process_request(entry)
          elsif is_a_response
            process_response(entry, ledger_entry)
          else
            reject!(entry, reason: "REQUEST_EXPIRED The request related to this entry has expired")
          end

          # In the normal Consumer workflow, `#check_for_entries` will pass the entry to any observers listening.
          # This is fine except Concurrent::TimerTask triggers the next execution before notifying the observers,
          # which causes `#check_for_entries` to be called before the entry is acknowledged and removed from
          # the stream.
          # The solution I decided to go with is to update the observers before TimerTask has a chance to and then
          # let TimerTask update the observers again, but with nil.
          # Code that utilizes `Consumer#add_callback` will never notice this, however, code utilizing
          # `Consumer#add_observer` will need to handle the `nil` "entry" that can be passed through
          nil
        ensure
          acknowledge_and_remove(entry) if entry
        end

        private

        def process_request(entry)
          # Hook into TimerTask to notify the observers manually before next execution. See above
          @task.instance_exec(entry) do |entry|
            # Concurrent::TimerTask passes [time, result, exception]
            observers.notify_observers { [Time.current, entry, nil] }
          end
        end

        def process_response(entry, ledger_entry)
          # ledger_entry#mailbox is type Concurrent::MVar. Currently Stream#track_and_send is waiting for a value
          # to be stored in here. Once this happens, that thread will receive this value and return it to the caller
          ledger_entry.mailbox.put(entry.content)
        end
      end
    end
  end
end
