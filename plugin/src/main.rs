//! bluetooth: the host's Bluetooth, through BlueZ, as a Yoke Plugin unit.
//!
//!   bluetooth [--default-agent]
//!
//! With `--default-agent` the Plugin's agent is the host's default, and is asked about every pairing,
//! a remote device's included: for a host with no desktop of its own. Without it, it is asked only about
//! the pairings the Plugin starts, and a desktop's own agent keeps the rest.

use std::sync::Arc;
use std::time::Duration;

use bluetooth::backend::{BluetoothBackend, Bluez};
use bluetooth::plugin::{declaration, handle, Bt};
use tokio::sync::broadcast::error::{RecvError, TryRecvError};
use yoke_sdk::plugin::{start, Event};

/// A discovery changes a device's signal strength many times a second; the Plugin reads again at most
/// this often.
const SETTLE: Duration = Duration::from_millis(700);

#[tokio::main]
async fn main() {
    if std::env::args().nth(1).as_deref() == Some("--manifest") {
        print!("{}", declaration().manifest());
        return;
    }
    let default_agent = std::env::args().skip(1).any(|a| a == "--default-agent");
    let unit = match start(&declaration()).await {
        Ok(unit) => Arc::new(unit),
        Err(err) => {
            eprintln!("bluetooth: {err}");
            std::process::exit(1);
        }
    };
    let found = Bluez::connect(default_agent).await;
    let (backend, mut asked) = match found {
        Ok((b, asked)) => (Ok(b as Arc<dyn BluetoothBackend>), Some(asked)),
        Err(e) => (Err(e), None),
    };
    let (grade, line) = match &backend {
        Ok(_) => (0, format!("BlueZ connected, the agent registered{}", if default_agent { " as the default" } else { "" })),
        Err(e) => (50, format!("no BlueZ: {e}")),
    };
    if let Err(err) = unit.health(grade, &line).await {
        eprintln!("bluetooth: {err}");
    }
    let mut said = backend.as_ref().ok().map(|b| b.subscribe());
    let bt = Bt::new(backend);
    if let Err(err) = bt.refresh(unit.as_ref()).await {
        eprintln!("bluetooth: {err}");
    }
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("bluetooth listens for SIGTERM");
    loop {
        tokio::select! {
            event = unit.next() => match event {
                None | Some(Event::Ended(_)) => break,
                Some(Event::Activated(a)) => {
                    if let Err(err) = bt.activated(unit.as_ref(), &a.stream).await {
                        eprintln!("bluetooth: {err}");
                    }
                }
                Some(Event::Stopped { stream }) => bt.stopped(&stream).await,
                // Each on its own task: a pairing waits for a person, and the answer is another command.
                Some(event) => {
                    let (unit, bt) = (unit.clone(), bt.clone());
                    tokio::spawn(async move {
                        if let Err(err) = handle(unit.as_ref(), &event, &bt).await {
                            eprintln!("bluetooth: {err}");
                        }
                    });
                }
            },
            ask = async { match asked.as_mut() { Some(rx) => rx.recv().await, None => std::future::pending().await } } => match ask {
                Some(ask) => {
                    if let Err(err) = bt.on_ask(unit.as_ref(), ask).await {
                        eprintln!("bluetooth: {err}");
                    }
                }
                None => asked = None,
            },
            ev = async { match said.as_mut() { Some(rx) => rx.recv().await, None => std::future::pending().await } } => match ev {
                Ok(()) | Err(RecvError::Lagged(_)) => {
                    tokio::time::sleep(SETTLE).await;
                    if let Some(rx) = said.as_mut() {
                        while !matches!(rx.try_recv(), Err(TryRecvError::Empty | TryRecvError::Closed)) {}
                    }
                    if let Err(err) = bt.refresh(unit.as_ref()).await {
                        eprintln!("bluetooth: {err}");
                    }
                }
                Err(RecvError::Closed) => {
                    said = None;
                    let _ = unit.health(50, "BlueZ went away").await;
                }
            },
            _ = term.recv() => {
                let _ = unit.close().await;
                while let Some(event) = unit.next().await {
                    if matches!(event, Event::Ended(_)) {
                        break;
                    }
                }
                break;
            }
        }
    }
    // A discovery this unit started does not outlive it.
    if bt.scanning() {
        bt.stopped(bluetooth::plugin::SCAN).await;
    }
}
