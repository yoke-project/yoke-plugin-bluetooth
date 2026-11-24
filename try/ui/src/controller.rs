//! Bridges the window and the bluetooth Plugin, through the interface contract alone.
//!
//! - **Reading.** `bluetooth.devices` is the whole picture at every change, and replaces the list.
//!   `bluetooth.status` is a stream of changes with a sequence: the panel subscribes first, then asks it,
//!   and drops every frame at or below the sequence it was answered with. The status carries the adapter
//!   and the pairing request waiting for an answer, which the window shows.
//! - **Discovering.** The Discover switch subscribes to `bluetooth.scan` and starts it; the Plugin
//!   discovers while it flows. Switched off, the panel releases it and stops it.
//! - **Acting.** Every button is a command; its acknowledgement is the message at the top. A pairing
//!   waits for the person: its request appears in the window, and Accept or Reject answers it.
//! - **No PIN is written**: the answer is built, sent and dropped.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use slint::{ComponentHandle, ModelRc, SharedString, VecModel, Weak};
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::time::{interval, sleep};
use yoke_sdk::interface::subscribed::Carries;
use yoke_sdk::interface::{Attachment, Filter, Options};

use crate::{DeviceRow, MainWindow};

const STATUS: &str = "bluetooth.status";
const DEVICES: &str = "bluetooth.devices";
const SCAN: &str = "bluetooth.scan";
const MAX_EVENTS: usize = 300;
const EVENTS_REDRAW: Duration = Duration::from_millis(500);
const AGAIN: Duration = Duration::from_secs(2);

#[derive(Default)]
struct Log {
    lines: VecDeque<String>,
    dirty: bool,
}

pub struct Shared {
    unit: String,
    attachment: OnceLock<Arc<Attachment>>,
    started: Mutex<Vec<String>>,
    /// Ends the scan's delivery when the switch goes off.
    scan: Mutex<Option<oneshot::Sender<()>>>,
}

#[derive(Clone)]
struct Ctx {
    at: Arc<Attachment>,
    unit: String,
    ui: Weak<MainWindow>,
    log: Arc<Mutex<Log>>,
    t0: Instant,
    shared: Arc<Shared>,
}

fn say(ui: &Weak<MainWindow>, msg: impl Into<String>) {
    let m = msg.into();
    // Also on stderr, so that a run nobody looks at — the watch's — can read what the panel showed.
    eprintln!("bluetooth-ui: {m}");
    let _ = ui.upgrade_in_event_loop(move |u| u.set_message(m.into()));
}

fn set_busy(ui: &Weak<MainWindow>, busy: bool) {
    let _ = ui.upgrade_in_event_loop(move |u| u.set_busy(busy));
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or("").to_string()
}

pub fn start(rt: Handle, address: String, unit: String, ui: &MainWindow) -> Arc<Shared> {
    let shared = Arc::new(Shared {
        unit: unit.clone(),
        attachment: OnceLock::new(),
        started: Mutex::new(Vec::new()),
        scan: Mutex::new(None),
    });
    let weak = ui.as_weak();
    let log = Arc::new(Mutex::new(Log::default()));
    let ready: Arc<OnceLock<Ctx>> = Arc::new(OnceLock::new());

    macro_rules! action {
        ($on:ident, ($($arg:ident),*), $body:expr) => {{
            let ready = ready.clone();
            let rt = rt.clone();
            let weak = weak.clone();
            ui.$on(move |$($arg),*| match ready.get() {
                Some(c) => { rt.spawn(($body)(c.clone(), $($arg),*)); }
                None => say(&weak, "Not attached to the application yet"),
            });
        }};
    }

    action!(on_set_power, (on), |c: Ctx, on: bool| async move {
        match command(&c, "bluetooth.power", json!({ "on": on })).await {
            Ok(line) => say(&c.ui, line),
            Err(e) => say(&c.ui, format!("Power failed: {e}")),
        }
    });

    action!(on_set_scan, (on), |c: Ctx, on: bool| async move {
        if on {
            scan_on(&c).await;
        } else {
            scan_off(&c).await;
        }
    });

    action!(on_pair, (address), |c: Ctx, address: SharedString| async move {
        set_busy(&c.ui, true);
        say(&c.ui, format!("Pairing with {address}..."));
        match command(&c, "bluetooth.pair", json!({ "address": address.to_string() })).await {
            Ok(line) => say(&c.ui, line),
            Err(e) => say(&c.ui, format!("Pairing with {address} failed: {e}")),
        }
        set_busy(&c.ui, false);
    });

    action!(on_connect_device, (address), |c: Ctx, address: SharedString| async move {
        set_busy(&c.ui, true);
        say(&c.ui, format!("Connecting to {address}..."));
        match command(&c, "bluetooth.connect", json!({ "address": address.to_string() })).await {
            Ok(line) => say(&c.ui, line),
            Err(e) => say(&c.ui, format!("Connecting to {address} failed: {e}")),
        }
        set_busy(&c.ui, false);
    });

    action!(on_disconnect_device, (address), |c: Ctx, address: SharedString| async move {
        match command(&c, "bluetooth.disconnect", json!({ "address": address.to_string() })).await {
            Ok(line) => say(&c.ui, line),
            Err(e) => say(&c.ui, format!("Disconnecting {address} failed: {e}")),
        }
    });

    action!(on_trust, (address, trusted), |c: Ctx, address: SharedString, trusted: bool| async move {
        match command(&c, "bluetooth.trust", json!({ "address": address.to_string(), "trusted": trusted })).await {
            Ok(line) => say(&c.ui, line),
            Err(e) => say(&c.ui, format!("Trust failed: {e}")),
        }
    });

    action!(on_forget, (address), |c: Ctx, address: SharedString| async move {
        match command(&c, "bluetooth.forget", json!({ "address": address.to_string() })).await {
            Ok(line) => say(&c.ui, line),
            Err(e) => say(&c.ui, format!("Forget failed: {e}")),
        }
    });

    action!(on_answer, (request, accept, code), |c: Ctx, request: SharedString, accept: bool, code: SharedString| async move {
        // The code goes as a PIN, and as a passkey too where it is one: the Plugin takes the one its
        // request asked for.
        let mut payload = json!({ "request_id": request.to_string(), "accept": accept });
        if accept && !code.is_empty() {
            payload["pin"] = json!(code.to_string());
            if let Ok(n) = code.trim().parse::<u32>() {
                if n <= 999_999 {
                    payload["passkey"] = json!(n);
                }
            }
        }
        match command(&c, "bluetooth.pairing.answer", payload).await {
            Ok(line) => say(&c.ui, line),
            Err(e) => say(&c.ui, format!("The answer was not taken: {e}")),
        }
    });

    {
        let (log, weak) = (log.clone(), weak.clone());
        ui.on_clear_events(move || {
            let mut l = log.lock().unwrap();
            l.lines.clear();
            l.dirty = false;
            let _ = weak.upgrade_in_event_loop(|u| u.set_events(ModelRc::new(VecModel::default())));
        });
    }

    let shared2 = shared.clone();
    rt.spawn(async move {
        let at = match Attachment::attach(&address, Options::new()).await {
            Ok(at) => Arc::new(at),
            Err(e) => {
                say(&weak, format!("Cannot attach to {address}: {e}"));
                return;
            }
        };
        let _ = shared2.attachment.set(at.clone());
        let c = Ctx { at, unit, ui: weak, log, t0: Instant::now(), shared: shared2 };
        let _ = ready.set(c.clone());
        say(&c.ui, "Attached");
        tokio::spawn(redraw_events(c.clone()));
        tokio::spawn(events(c.clone()));
        tokio::spawn(follow(c.clone(), DEVICES));
        tokio::spawn(follow(c.clone(), STATUS));
        if let Ok(v) = ask(&c, DEVICES).await {
            show_devices(&c, &v["devices"]);
        }
    });
    shared
}

/// Once the window has closed: discovery and the streams this panel started are stopped.
pub async fn finish(shared: Arc<Shared>) {
    let Some(at) = shared.attachment.get() else { return };
    if let Some(stop) = shared.scan.lock().unwrap().take() {
        let _ = stop.send(());
    }
    let started = std::mem::take(&mut *shared.started.lock().unwrap());
    for stream in started {
        let _ = tokio::time::timeout(Duration::from_secs(2), at.stop_stream(&shared.unit, &stream)).await;
    }
}

/// Sends a command and returns the line it was done with; anything else is an error with its line.
async fn command(c: &Ctx, r#type: &str, payload: Value) -> Result<String, String> {
    let bytes = payload.to_string().into_bytes();
    drop(payload);
    match c.at.command(&c.unit, r#type, &bytes).await {
        Ok(ack) => match ack.outcome {
            2 => Ok(ack.line),
            1 => Err(format!("still running — {}", ack.line)),
            _ => Err(ack.line),
        },
        Err(e) => Err(e.to_string()),
    }
}

async fn ask(c: &Ctx, r#type: &str) -> Result<Value, String> {
    let answer = c.at.ask(&c.unit, r#type, b"").await.map_err(|e| e.to_string())?;
    serde_json::from_slice(&answer).map_err(|e| e.to_string())
}

fn log(c: &Ctx, line: String) {
    let mut l = c.log.lock().unwrap();
    l.lines.push_front(format!("+{:>8.3}s  {line}", c.t0.elapsed().as_secs_f32()));
    l.lines.truncate(MAX_EVENTS);
    l.dirty = true;
}

async fn scan_on(c: &Ctx) {
    let mut delivery = match c.at.subscribe_stream(&c.unit, SCAN).await {
        Ok(d) => d,
        Err(e) => {
            say(&c.ui, format!("Cannot discover: {e}"));
            let _ = c.ui.upgrade_in_event_loop(|u| u.set_scanning(false));
            return;
        }
    };
    if !delivery.flowing {
        match c.at.start_stream(&c.unit, SCAN).await {
            Ok(_) => c.shared.started.lock().unwrap().push(SCAN.into()),
            Err(e) => {
                say(&c.ui, format!("Cannot discover: {e}"));
                let _ = c.ui.upgrade_in_event_loop(|u| u.set_scanning(false));
                return;
            }
        }
    }
    say(&c.ui, "Discovering");
    let (stop, mut stopped) = oneshot::channel();
    *c.shared.scan.lock().unwrap() = Some(stop);
    // The devices found come on `bluetooth.devices` as well; the scan's frames are read and dropped, so
    // that the delivery is not released for falling behind.
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stopped => break,
                frame = delivery.next() => if !matches!(frame, Ok(Some(_))) { break },
            }
        }
        let _ = delivery.release().await;
    });
}

async fn scan_off(c: &Ctx) {
    if let Some(stop) = c.shared.scan.lock().unwrap().take() {
        let _ = stop.send(());
    }
    let was_ours = {
        let mut started = c.shared.started.lock().unwrap();
        let before = started.len();
        started.retain(|s| s != SCAN);
        started.len() != before
    };
    if was_ours {
        let _ = c.at.stop_stream(&c.unit, SCAN).await;
    }
    say(&c.ui, "Discovery stopped");
}

async fn follow(c: Ctx, stream: &'static str) {
    loop {
        match c.at.subscribe_stream(&c.unit, stream).await {
            Ok(mut delivery) => {
                if !delivery.flowing {
                    match c.at.start_stream(&c.unit, stream).await {
                        Ok(_) => c.shared.started.lock().unwrap().push(stream.to_string()),
                        Err(e) => log(&c, format!("{stream} cannot be started: {e}")),
                    }
                }
                let mut seen = 0u64;
                if stream == STATUS {
                    match ask(&c, STATUS).await {
                        Ok(v) => {
                            seen = v["sequence"].as_u64().unwrap_or(0);
                            on_status(&c, &v);
                        }
                        Err(e) => log(&c, format!("{STATUS} cannot be asked: {e}")),
                    }
                }
                loop {
                    match delivery.next().await {
                        Ok(Some(frame)) => {
                            let Ok(v) = serde_json::from_slice::<Value>(&frame.payload) else { continue };
                            if stream == DEVICES {
                                show_devices(&c, &v["devices"]);
                            } else {
                                let sequence = v["sequence"].as_u64().unwrap_or(0);
                                if sequence > seen {
                                    seen = sequence;
                                    on_status(&c, &v);
                                }
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            log(&c, format!("{stream}: {e}"));
                            break;
                        }
                    }
                }
                log(&c, format!("{stream}: the delivery ended; subscribing again"));
            }
            Err(e) => log(&c, format!("{stream} cannot be subscribed to: {e}")),
        }
        sleep(AGAIN).await;
    }
}

fn on_status(c: &Ctx, v: &Value) {
    let a = &v["adapter"];
    let adapter = if a.is_null() { String::new() } else { format!("{} ({})", s(&a["alias"]), s(&a["address"])) };
    let powered = a["powered"].as_bool().unwrap_or(false);
    let discovering = a["discovering"].as_bool().unwrap_or(false);
    let p = &v["pairing"];
    let (id, kind, device, passkey, answers) = if p.is_null() {
        Default::default()
    } else {
        let device = match p["alias"].as_str() {
            Some(alias) if !alias.is_empty() => format!("{alias} ({})", s(&p["address"])),
            _ => s(&p["address"]),
        };
        (s(&p["request_id"]), s(&p["kind"]), device, s(&p["passkey"]), p["answers"].as_bool().unwrap_or(false))
    };
    let pairing = if id.is_empty() { String::new() } else { format!(", pairing {id} {kind}") };
    log(c, format!("status   #{} powered={powered} discovering={discovering}{pairing}", v["sequence"]));
    let _ = c.ui.upgrade_in_event_loop(move |u| {
        u.set_adapter(adapter.into());
        u.set_powered(powered);
        u.set_discovering(discovering);
        u.set_pairing_id(id.into());
        u.set_pairing_kind(kind.into());
        u.set_pairing_device(device.into());
        u.set_pairing_passkey(passkey.into());
        u.set_pairing_answers(answers);
    });
}

async fn events(c: Ctx) {
    let filter = Filter { subject_kind: "unit".into(), subject_identity: c.unit.clone(), ..Default::default() };
    let mut sub = match c.at.subscribe(filter).await {
        Ok(s) => s,
        Err(e) => {
            log(&c, format!("events cannot be subscribed to: {e}"));
            return;
        }
    };
    loop {
        match sub.next().await {
            Ok(Some(sub)) => match sub.carries {
                Some(Carries::Event(e)) => {
                    let detail: Value = serde_json::from_slice(&e.detail).unwrap_or(Value::Null);
                    let line = detail["line"].as_str().map(str::to_string).unwrap_or_else(|| {
                        let d = detail.to_string();
                        if d.len() > 160 { format!("{}…", &d[..160]) } else { d }
                    });
                    let class = if e.occurrence.is_empty() { String::new() } else { format!(" {}", e.occurrence) };
                    log(&c, format!("event    {} {}{class} [{}] {line}", e.seq, e.r#type, e.severity));
                    if e.occurrence == "bluetooth.job.finished" {
                        say(&c.ui, line);
                    }
                }
                Some(Carries::Overflow(_)) => log(&c, "events: the panel fell behind and some were dropped".into()),
                _ => {}
            },
            Ok(None) => break,
            Err(e) => {
                log(&c, format!("events: {e}"));
                break;
            }
        }
    }
}

async fn redraw_events(c: Ctx) {
    let mut tick = interval(EVENTS_REDRAW);
    loop {
        tick.tick().await;
        let rows = {
            let mut l = c.log.lock().unwrap();
            if !l.dirty {
                continue;
            }
            l.dirty = false;
            l.lines.iter().map(|s| SharedString::from(s.as_str())).collect::<Vec<_>>()
        };
        let _ = c.ui.upgrade_in_event_loop(move |u| u.set_events(ModelRc::new(VecModel::from(rows))));
    }
}

/// Connected first, then paired, then the strongest signal.
fn show_devices(c: &Ctx, devices: &Value) {
    let mut list = devices.as_array().cloned().unwrap_or_default();
    let flag = |d: &Value, k: &str| d[k].as_bool().unwrap_or(false);
    list.sort_by(|a, b| {
        flag(b, "connected")
            .cmp(&flag(a, "connected"))
            .then(flag(b, "paired").cmp(&flag(a, "paired")))
            .then(b["rssi"].as_i64().unwrap_or(-200).cmp(&a["rssi"].as_i64().unwrap_or(-200)))
    });
    let rows: Vec<DeviceRow> = list
        .iter()
        .map(|d| DeviceRow {
            address: s(&d["address"]).into(),
            alias: s(&d["alias"]).into(),
            icon: s(&d["icon"]).into(),
            paired: flag(d, "paired"),
            trusted: flag(d, "trusted"),
            connected: flag(d, "connected"),
            rssi: d["rssi"].as_i64().map(|r| r.to_string()).unwrap_or_default().into(),
            battery: d["battery"].as_u64().map(|b| b.to_string()).unwrap_or_default().into(),
        })
        .collect();
    let _ = c.ui.upgrade_in_event_loop(move |u| {
        if u.get_message() == "Starting..." {
            u.set_message("Ready".into());
        }
        u.set_devices(ModelRc::new(VecModel::from(rows)));
    });
}
