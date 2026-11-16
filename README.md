# bluetooth

A Yoke Plugin that reads and drives the host's Bluetooth through BlueZ: the adapter, the devices known
and in range, their batteries, and pairing — a PIN, a passkey or a confirmation asked of the person at
the interface.

| | |
| --- | --- |
| questions | `bluetooth.status` (the adapter, the devices connected, the pairing request waiting, with its sequence), `bluetooth.adapters`, `bluetooth.devices` |
| commands | `bluetooth.power` `{on, adapter?}`, `bluetooth.pair`, `bluetooth.connect`, `bluetooth.disconnect`, `bluetooth.forget` `{address}`, `bluetooth.trust` `{address, trusted?}`, `bluetooth.pairing.answer` `{request_id, accept, pin?, passkey?}` |
| streams | `bluetooth.status`: every change, none lost; `bluetooth.devices`: every device, whole, at every change; `bluetooth.scan`: the devices in range that are not paired — the adapter discovers only while it flows |
| occurrences | `bluetooth.device.connected`, `bluetooth.device.disconnected` (10), `bluetooth.pairing.request`, `bluetooth.pairing.display` (30), `bluetooth.job.finished` when a command acknowledged as still running ends |

Every payload is JSON and states `schema`; a question takes `{}`. Every occurrence carries its facts as
JSON in its detail, which is what an interface client receives.

## Pairing, and the person

A Plugin is passive: it never starts a conversation. When BlueZ needs the person, the Plugin's agent:

1. reports `bluetooth.pairing.request`, its detail naming the `request_id`, the device, the `kind` asked —
   `pin`, `passkey`, `confirm` (with the `passkey` both sides show), `authorize` or `service` — and
   whether it `answers`;
2. puts the same in `bluetooth.status` under `pairing`, so that an interface attaching late sees it;
3. waits for `bluetooth.pairing.answer` naming that `request_id`: `accept`, with the `pin` or the
   `passkey` where the kind asks for one. A request no one answers within 60 s is rejected.

A code the device shows, to be typed on the other side, is `bluetooth.pairing.display`, and needs no
answer. A PIN an answer carries is handed to BlueZ and written nowhere.

`bluetooth.pair` waits for the person: it is acknowledged as accepted after 25 s, and
`bluetooth.job.finished` says how it ended.

By default the agent is asked only about the pairings the Plugin starts, and a desktop's own agent keeps
the rest. With `--default-agent` among the unit's arguments it is the host's default and is asked about
every pairing, a remote device's included: for a host with no desktop of its own.

**An interface that attaches late** subscribes to `bluetooth.status`, then asks it, and drops what the
stream brings at or below the sequence it was answered with.

Take it into an application with `yoke-compose add plugin https://github.com/yoke-project/yoke-plugin-bluetooth@<tag>`.
Build it with `yoke-compose build`, and try it from `try/`.

Licensed under the Apache License 2.0.
