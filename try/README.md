# Trying bluetooth

This directory is an application project that uses the Plugin in `..`: `yoke-compose init` made it and
`add plugin .. --yes` took the Plugin into it. Nothing here is the Plugin, and taking the Plugin into
another application never takes this directory.

| Path | What it is |
| --- | --- |
| `yoke.yaml` | the application: the unit `bluetooth` and the channel `panel`, a local socket for one client |
| `ui/` | the panel: an interface client written with Slint and the Rust interface library. It shows the adapter and the devices, discovers while its switch is on, pairs, connects, and asks the person when a pairing needs them. It reaches the Plugin through the contract alone and links none of its code |
| `follow/` | an interface client: it asks every question, follows every stream and every event of the unit, and prints each as one line |
| `run.sh` | runs the application on a local Core, grants what the Plugin declares, and opens the panel, or follows the unit with `--follow` |

```sh
./run.sh                        # the panel; closing it stops everything
./run.sh --read-only            # no command granted: the panel reads, and the Core refuses every button
./run.sh --follow --seconds 10  # one line per answer, frame and event, in place of the panel
```

With `--follow`, following `bluetooth.scan` makes the adapter discover for as long as `run.sh` runs, and a
pairing goes from another terminal, its request appearing in what `run.sh` prints:

    echo '{"address":"AA:BB:CC:DD:EE:FF"}' | YOKE_INSTANCE=<instance> yoke unit command bluetooth bluetooth.pair --input -
    echo '{"request_id":"pair-1","accept":true}' | YOKE_INSTANCE=<instance> yoke unit command bluetooth bluetooth.pairing.answer --input -
