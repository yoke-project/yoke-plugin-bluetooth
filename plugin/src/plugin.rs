//! The bluetooth Plugin: the adapter, the devices and their pairing, as questions, commands, streams and
//! occurrences.
//!
//! - **`bluetooth.status` is a question and a stream of the same record**, with a sequence: the adapter,
//!   the devices connected, and the pairing request waiting for an answer, if one is. A client that
//!   subscribes and then asks holds the present state, and drops what the stream brings at or below the
//!   sequence it was answered with.
//! - **A pairing that needs the person is asked through the interface**: BlueZ asks the Plugin's agent for
//!   a PIN, a passkey, a confirmation or an authorization; the Plugin reports
//!   `bluetooth.pairing.request` with the request's identity and what is asked, puts it in the status,
//!   and waits for `bluetooth.pairing.answer` naming that identity. A request no one answers within
//!   [`PAIRING_WAIT`] is rejected. A code the device shows and the person types on the other side is
//!   reported as `bluetooth.pairing.display`, and needs no answer.
//! - **Discovery runs while `bluetooth.scan` flows**, and stops when it stops: a radio scanning for nobody
//!   is a battery spent for nothing.
//! - **Every occurrence carries its facts as JSON in its detail**, which is what an interface client
//!   receives; the line is for the log. A PIN an answer carries is handed to BlueZ and written nowhere.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::oneshot;
use yoke_sdk::plugin::{Capability, Command, Declaration, Event, Object, Outcome, Question, Severity, Stream, Unit};

use crate::backend::{Adapter, Ask, BluetoothBackend, BtError, Device, Kind, Reply};

pub const PLUGIN: &str = "com.yoke.bluetooth";
pub const SCHEMA: u32 = 1;

pub const STATUS: &str = "bluetooth.status";
pub const ADAPTERS: &str = "bluetooth.adapters";
pub const DEVICES: &str = "bluetooth.devices";
pub const QUERIES: [&str; 3] = [STATUS, ADAPTERS, DEVICES];

pub const POWER: &str = "bluetooth.power";
pub const PAIR: &str = "bluetooth.pair";
pub const CONNECT: &str = "bluetooth.connect";
pub const DISCONNECT: &str = "bluetooth.disconnect";
pub const TRUST: &str = "bluetooth.trust";
pub const FORGET: &str = "bluetooth.forget";
pub const ANSWER: &str = "bluetooth.pairing.answer";
pub const COMMANDS: [&str; 7] = [POWER, PAIR, CONNECT, DISCONNECT, TRUST, FORGET, ANSWER];

/// The devices in range that are not paired, while it flows; discovery runs as long.
pub const SCAN: &str = "bluetooth.scan";
pub const STREAMS: [&str; 3] = [STATUS, DEVICES, SCAN];

pub const CONNECTED: &str = "bluetooth.device.connected";
pub const DISCONNECTED: &str = "bluetooth.device.disconnected";
pub const PAIRING_REQUEST: &str = "bluetooth.pairing.request";
pub const PAIRING_DISPLAY: &str = "bluetooth.pairing.display";
pub const JOB_FINISHED: &str = "bluetooth.job.finished";
pub const OCCURRENCES: [&str; 5] = [CONNECTED, DISCONNECTED, PAIRING_REQUEST, PAIRING_DISPLAY, JOB_FINISHED];

/// How long a command waits before it is acknowledged as accepted and still running: a pairing waits for
/// a person.
pub const COMMAND_WAIT: Duration = Duration::from_secs(25);
/// How long a pairing request waits for its answer before it is rejected.
pub const PAIRING_WAIT: Duration = Duration::from_secs(60);

pub fn declaration() -> Declaration {
    let answer = |q: &&str| Capability { name: format!("query.{q}.answer"), governs: Object::Query(q.to_string()) };
    let accept = |c: &&str| Capability { name: format!("command.{c}.accept"), governs: Object::Command(c.to_string()) };
    let publish = |s: &&str| Capability { name: format!("stream.{s}.publish"), governs: Object::Stream(s.to_string()) };
    let report = |o: &&str| Capability { name: format!("event.{o}.report"), governs: Object::Occurrence(o.to_string()) };
    Declaration {
        id: PLUGIN.into(),
        queries: QUERIES.iter().map(|q| q.to_string()).collect(),
        commands: COMMANDS.iter().map(|c| c.to_string()).collect(),
        streams: vec![
            // A pairing request is not one a client may miss.
            Stream { id: STATUS.into(), tolerates_loss: false, tolerates_reorder: false, max_frame: 0 },
            // Each of these two is the whole picture again, so a lost one is made good by the next.
            Stream { id: DEVICES.into(), tolerates_loss: true, tolerates_reorder: false, max_frame: 0 },
            Stream { id: SCAN.into(), tolerates_loss: true, tolerates_reorder: false, max_frame: 0 },
        ],
        occurrences: OCCURRENCES.iter().map(|o| o.to_string()).collect(),
        capabilities: QUERIES
            .iter()
            .map(answer)
            .chain(COMMANDS.iter().map(accept))
            .chain(STREAMS.iter().map(publish))
            .chain(OCCURRENCES.iter().map(report))
            .collect(),
        ..Default::default()
    }
}

#[derive(Debug)]
pub enum Error {
    Invalid(String),
    /// No pairing request of that identity waits.
    NoRequest(String),
    Bt(BtError),
}

impl From<BtError> for Error {
    fn from(e: BtError) -> Self {
        Error::Bt(e)
    }
}

pub fn code(e: &Error) -> &'static str {
    match e {
        Error::Invalid(_) => "invalid",
        Error::NoRequest(_) => "not_found",
        Error::Bt(BtError::Unavailable(_)) => "unavailable",
        Error::Bt(BtError::NotFound(_)) => "not_found",
        Error::Bt(BtError::PermissionDenied) => "permission_denied",
        Error::Bt(BtError::Bluez { name, .. }) => match name.as_str() {
            "AuthenticationFailed" | "AuthenticationRejected" | "AuthenticationCanceled" | "AuthenticationTimeout" => "auth_failed",
            "AlreadyExists" => "already_exists",
            "InProgress" => "in_progress",
            "NotReady" => "not_ready",
            "NotSupported" => "unsupported",
            _ => "backend",
        },
        Error::Bt(BtError::Bus(_)) => "backend",
    }
}

pub fn message(e: &Error) -> String {
    match e {
        Error::Invalid(m) | Error::NoRequest(m) => m.clone(),
        Error::Bt(e) => e.to_string(),
    }
}

fn input<T: DeserializeOwned>(what: &str, payload: &[u8]) -> Result<T, Error> {
    let payload = if payload.iter().all(u8::is_ascii_whitespace) { b"{}".as_slice() } else { payload };
    serde_json::from_slice(payload).map_err(|e| {
        let kind = match e.classify() {
            serde_json::error::Category::Data => "a field is missing, unknown or of the wrong type",
            _ => "it is not JSON",
        };
        Error::Invalid(format!("the payload is not what {what} takes: {kind}, at line {} column {}", e.line(), e.column()))
    })
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NoInput {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PowerInput {
    pub on: bool,
    pub adapter: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AddressInput {
    pub address: String,
}

fn yes() -> bool {
    true
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrustInput {
    pub address: String,
    #[serde(default = "yes")]
    pub trusted: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnswerInput {
    pub request_id: String,
    pub accept: bool,
    /// For a request of kind `pin`.
    pub pin: Option<String>,
    /// For a request of kind `passkey`: six digits at most.
    pub passkey: Option<u32>,
}

/// An address, as BlueZ writes it: six pairs of hexadecimal digits, upper case.
fn address(a: &str) -> Result<String, Error> {
    let up = a.trim().to_ascii_uppercase();
    let ok = up.len() == 17
        && up.split(':').count() == 6
        && up.split(':').all(|p| p.len() == 2 && p.chars().all(|c| c.is_ascii_hexdigit()));
    if ok {
        Ok(up)
    } else {
        Err(Error::Invalid("an address is six pairs of hexadecimal digits, as AA:BB:CC:DD:EE:FF".into()))
    }
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

pub fn adapter_json(a: &Adapter) -> Value {
    json!({
        "id": a.id, "address": a.address, "alias": a.alias, "powered": a.powered, "discovering": a.discovering,
        "pairable": a.pairable, "discoverable": a.discoverable,
    })
}

pub fn device_json(d: &Device) -> Value {
    json!({
        "address": d.address, "adapter": d.adapter, "alias": d.alias, "icon": d.icon, "paired": d.paired,
        "trusted": d.trusted, "blocked": d.blocked, "connected": d.connected, "rssi": d.rssi, "battery": d.battery,
    })
}

/// A pairing request in flight.
struct Pending {
    id: String,
    address: String,
    kind: Kind,
    /// `None` for a code to show, which is not answered.
    reply: Option<oneshot::Sender<Reply>>,
}

impl Pending {
    fn json(&self, alias: Option<&str>) -> Value {
        let mut v = json!({
            "request_id": self.id, "address": self.address, "alias": alias, "kind": self.kind.name(),
            "answers": self.reply.is_some(),
        });
        match &self.kind {
            Kind::Confirm { passkey } => v["passkey"] = json!(format!("{passkey:06}")),
            Kind::DisplayPasskey { passkey, entered } => {
                v["passkey"] = json!(format!("{passkey:06}"));
                v["entered"] = json!(entered);
            }
            Kind::DisplayPin { pin } => v["pin"] = json!(pin),
            Kind::Service { uuid } => v["service"] = json!(uuid),
            _ => {}
        }
        v
    }
}

pub trait Acts {
    fn answer(&self, q: &Question, payload: Vec<u8>) -> impl std::future::Future<Output = Result<(), yoke_sdk::base::Error>>;
    fn fail(&self, q: &Question, code: &str, message: &str) -> impl std::future::Future<Output = Result<(), yoke_sdk::base::Error>>;
    fn ack(&self, c: &Command, outcome: Outcome, line: &str) -> impl std::future::Future<Output = Result<(), yoke_sdk::base::Error>>;
    fn report(&self, occurrence: &str, severity: u8, line: &str, detail: Value) -> impl std::future::Future<Output = Result<(), yoke_sdk::base::Error>>;
    fn emit(&self, stream: &str, payload: Vec<u8>) -> impl std::future::Future<Output = Result<(), yoke_sdk::base::Error>>;
}

impl Acts for Unit {
    async fn answer(&self, q: &Question, payload: Vec<u8>) -> Result<(), yoke_sdk::base::Error> {
        Unit::answer(self, q, payload).await
    }
    async fn fail(&self, q: &Question, code: &str, message: &str) -> Result<(), yoke_sdk::base::Error> {
        Unit::fail(self, &q.id, code, message).await
    }
    async fn ack(&self, c: &Command, outcome: Outcome, line: &str) -> Result<(), yoke_sdk::base::Error> {
        Unit::ack(self, c, outcome, line).await
    }
    async fn report(&self, occurrence: &str, severity: u8, line: &str, detail: Value) -> Result<(), yoke_sdk::base::Error> {
        Unit::report(self, occurrence, Some(Severity::of(severity)), line, detail.to_string().into_bytes()).await
    }
    async fn emit(&self, stream: &str, payload: Vec<u8>) -> Result<(), yoke_sdk::base::Error> {
        Unit::emit(self, stream, payload).await
    }
}

struct Status {
    sequence: u64,
    body: Value,
    at: u64,
}

impl Status {
    fn record(&self) -> Value {
        let mut v = json!({ "schema": SCHEMA, "sequence": self.sequence, "at": self.at });
        if let (Some(v), Some(body)) = (v.as_object_mut(), self.body.as_object()) {
            v.extend(body.clone());
        }
        v
    }
}

pub type Backend = Result<Arc<dyn BluetoothBackend>, BtError>;

pub struct Bt {
    pub backend: Backend,
    status: Mutex<Status>,
    /// The devices connected at the last reading, by address; `None` before the first.
    connected: Mutex<Option<BTreeMap<String, String>>>,
    devices: Mutex<String>,
    scanned: Mutex<String>,
    pending: Mutex<Option<Pending>>,
    requests: AtomicU64,
    status_flows: AtomicBool,
    devices_flow: AtomicBool,
    scan_flows: AtomicBool,
}

impl Bt {
    pub fn new(backend: Backend) -> Arc<Bt> {
        Arc::new(Bt {
            backend,
            status: Mutex::new(Status { sequence: 0, body: Value::Null, at: now_ms() }),
            connected: Mutex::new(None),
            devices: Mutex::new(String::new()),
            scanned: Mutex::new(String::new()),
            pending: Mutex::new(None),
            requests: AtomicU64::new(0),
            status_flows: AtomicBool::new(false),
            devices_flow: AtomicBool::new(false),
            scan_flows: AtomicBool::new(false),
        })
    }

    fn bluez(&self) -> Result<&dyn BluetoothBackend, Error> {
        match &self.backend {
            Ok(b) => Ok(b.as_ref()),
            Err(e) => Err(Error::Bt(BtError::Unavailable(e.to_string()))),
        }
    }

    fn flag(&self, stream: &str) -> Option<&AtomicBool> {
        match stream {
            STATUS => Some(&self.status_flows),
            DEVICES => Some(&self.devices_flow),
            SCAN => Some(&self.scan_flows),
            _ => None,
        }
    }

    pub fn scanning(&self) -> bool {
        self.scan_flows.load(Ordering::Relaxed)
    }

    /// Reads BlueZ again and says what changed: the status counted and emitted where it flows, a device
    /// connected or disconnected reported, the devices and the scan emitted where they flow and changed. The
    /// first reading counts as sequence 1 and reports nothing.
    pub async fn refresh(&self, u: &impl Acts) -> Result<(), yoke_sdk::base::Error> {
        let Ok(b) = &self.backend else { return Ok(()) };
        let (Ok(adapters), Ok(devices)) = (b.adapters().await, b.devices().await) else { return Ok(()) };
        // A code shown for a device that has since paired is shown no more.
        {
            let mut pending = self.pending.lock().unwrap();
            let done = pending.as_ref().is_some_and(|p| {
                p.reply.is_none() && devices.iter().any(|d| d.address == p.address && d.paired)
            });
            if done {
                *pending = None;
            }
        }
        let mut first = self.status(u, &adapters, &devices).await;
        first = first.and(self.connections(u, &devices).await);
        if self.devices_flow.load(Ordering::Relaxed) {
            first = first.and(self.emit_devices(u, &devices, false).await);
        }
        if self.scanning() {
            first = first.and(self.emit_scan(u, &devices, false).await);
        }
        first
    }

    async fn status(&self, u: &impl Acts, adapters: &[Adapter], devices: &[Device]) -> Result<(), yoke_sdk::base::Error> {
        let alias = |a: &str| devices.iter().find(|d| d.address == a).map(|d| d.alias.clone());
        let pairing = self.pending.lock().unwrap().as_ref().map(|p| p.json(alias(&p.address).as_deref()));
        let connected: Vec<Value> = devices
            .iter()
            .filter(|d| d.connected)
            .map(|d| json!({ "address": d.address, "alias": d.alias, "icon": d.icon, "battery": d.battery }))
            .collect();
        let adapter = adapters.first().map(|a| {
            json!({ "id": a.id, "address": a.address, "alias": a.alias, "powered": a.powered, "discovering": a.discovering, "pairable": a.pairable })
        });
        let next = json!({ "adapter": adapter, "connected": connected, "pairing": pairing });
        let record = {
            let mut status = self.status.lock().unwrap();
            if status.body == next {
                return Ok(());
            }
            status.body = next;
            status.sequence += 1;
            status.at = now_ms();
            status.record()
        };
        if self.status_flows.load(Ordering::Relaxed) {
            u.emit(STATUS, record.to_string().into_bytes()).await
        } else {
            Ok(())
        }
    }

    async fn connections(&self, u: &impl Acts, devices: &[Device]) -> Result<(), yoke_sdk::base::Error> {
        let now: BTreeMap<String, String> =
            devices.iter().filter(|d| d.connected).map(|d| (d.address.clone(), d.alias.clone())).collect();
        let before = self.connected.lock().unwrap().replace(now.clone());
        let Some(before) = before else { return Ok(()) };
        let mut first = Ok(());
        for (address, alias) in now.iter().filter(|(a, _)| !before.contains_key(*a)) {
            let line = format!("{alias} ({address}) connected");
            let detail = json!({ "schema": SCHEMA, "line": line, "address": address, "alias": alias });
            first = first.and(u.report(CONNECTED, 10, &line, detail).await);
        }
        for (address, alias) in before.iter().filter(|(a, _)| !now.contains_key(*a)) {
            let line = format!("{alias} ({address}) disconnected");
            let detail = json!({ "schema": SCHEMA, "line": line, "address": address, "alias": alias });
            first = first.and(u.report(DISCONNECTED, 10, &line, detail).await);
        }
        first
    }

    async fn emit_devices(&self, u: &impl Acts, devices: &[Device], always: bool) -> Result<(), yoke_sdk::base::Error> {
        let list: Vec<Value> = devices.iter().map(device_json).collect();
        let picture = Value::Array(list.clone()).to_string();
        {
            let mut last = self.devices.lock().unwrap();
            if !always && *last == picture {
                return Ok(());
            }
            *last = picture;
        }
        u.emit(DEVICES, json!({ "schema": SCHEMA, "at": now_ms(), "devices": list }).to_string().into_bytes()).await
    }

    /// The devices in range that are not paired: those discovery found.
    async fn emit_scan(&self, u: &impl Acts, devices: &[Device], always: bool) -> Result<(), yoke_sdk::base::Error> {
        let list: Vec<Value> = devices.iter().filter(|d| !d.paired && d.rssi.is_some()).map(device_json).collect();
        let picture = Value::Array(list.clone()).to_string();
        {
            let mut last = self.scanned.lock().unwrap();
            if !always && *last == picture {
                return Ok(());
            }
            *last = picture;
        }
        u.emit(SCAN, json!({ "schema": SCHEMA, "at": now_ms(), "devices": list }).to_string().into_bytes()).await
    }

    /// A stream was activated: it is given the present picture at once, and discovery starts where it is
    /// the scan.
    pub async fn activated(&self, u: &impl Acts, stream: &str) -> Result<(), yoke_sdk::base::Error> {
        let Some(flag) = self.flag(stream) else { return Ok(()) };
        flag.store(true, Ordering::Relaxed);
        if stream == STATUS {
            let record = self.status.lock().unwrap().record();
            return u.emit(STATUS, record.to_string().into_bytes()).await;
        }
        let Ok(b) = &self.backend else { return Ok(()) };
        if stream == SCAN {
            if let Err(e) = b.start_discovery().await {
                let _ = u.emit(SCAN, json!({ "schema": SCHEMA, "at": now_ms(), "devices": [], "error": e.to_string() }).to_string().into_bytes()).await;
            }
        }
        let Ok(devices) = b.devices().await else { return Ok(()) };
        if stream == SCAN {
            self.emit_scan(u, &devices, true).await
        } else {
            self.emit_devices(u, &devices, true).await
        }
    }

    pub async fn stopped(&self, stream: &str) {
        if let Some(flag) = self.flag(stream) {
            flag.store(false, Ordering::Relaxed);
        }
        if stream == SCAN {
            if let Ok(b) = &self.backend {
                let _ = b.stop_discovery().await;
            }
        }
    }

    /// What the agent was asked. A request that needs an answer waits in the status until it is answered,
    /// cancelled or left too long; a code to show stays there until the device has paired.
    pub async fn on_ask(self: &Arc<Self>, u: &impl Acts, ask: Ask) -> Result<(), yoke_sdk::base::Error> {
        match ask {
            Ask::Cancel => {
                // Dropping the sender rejects, and BlueZ already gave up.
                self.pending.lock().unwrap().take();
            }
            Ask::Request { address, kind, reply } => {
                let id = format!("pair-{}", self.requests.fetch_add(1, Ordering::Relaxed) + 1);
                let answers = reply.is_some();
                let pending = Pending { id: id.clone(), address: address.clone(), kind: kind.clone(), reply };
                let detail = {
                    let mut d = pending.json(None);
                    d["schema"] = json!(SCHEMA);
                    d
                };
                // A new request replaces one in flight: BlueZ asks one at a time.
                *self.pending.lock().unwrap() = Some(pending);
                let (occurrence, line) = match &kind {
                    Kind::Pin => (PAIRING_REQUEST, format!("{address} asks for a PIN code ({id})")),
                    Kind::Passkey => (PAIRING_REQUEST, format!("{address} asks for the passkey it shows ({id})")),
                    Kind::Confirm { passkey } => (PAIRING_REQUEST, format!("{address}: confirm the passkey {passkey:06} ({id})")),
                    Kind::Authorize => (PAIRING_REQUEST, format!("{address} asks to pair ({id})")),
                    Kind::Service { uuid } => (PAIRING_REQUEST, format!("{address} asks to use the service {uuid} ({id})")),
                    Kind::DisplayPin { .. } => (PAIRING_DISPLAY, format!("type the PIN shown on {address}")),
                    Kind::DisplayPasskey { .. } => (PAIRING_DISPLAY, format!("type the passkey shown on {address}")),
                };
                let mut detail = detail;
                detail["line"] = json!(line);
                let mut first = u.report(occurrence, 30, &line, detail).await;
                if answers {
                    let me = self.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(PAIRING_WAIT).await;
                        let mut pending = me.pending.lock().unwrap();
                        if pending.as_ref().is_some_and(|p| p.id == id) {
                            pending.take();
                        }
                    });
                }
                first = first.and(self.refresh(u).await);
                return first;
            }
        }
        self.refresh(u).await
    }

    /// Answers the request in flight. A request answered with what it did not ask for is rejected.
    fn answer_pairing(&self, i: AnswerInput) -> Result<String, Error> {
        let mut pending = self.pending.lock().unwrap();
        let Some(p) = pending.as_ref().filter(|p| p.id == i.request_id && p.reply.is_some()) else {
            return Err(Error::NoRequest(format!("no pairing request {} waits for an answer", i.request_id)));
        };
        let reply = match (&p.kind, i.accept) {
            (_, false) => None,
            (Kind::Pin, true) => match i.pin {
                Some(pin) if !pin.is_empty() && pin.len() <= 16 => Some(Reply::Pin(pin)),
                _ => return Err(Error::Invalid("a pin request is answered with a pin of 1 to 16 characters".into())),
            },
            (Kind::Passkey, true) => match i.passkey {
                Some(k) if k <= 999_999 => Some(Reply::Passkey(k)),
                _ => return Err(Error::Invalid("a passkey request is answered with a passkey of six digits".into())),
            },
            (_, true) => Some(Reply::Accept),
        };
        let p = pending.take().expect("held above");
        let (id, address) = (p.id.clone(), p.address.clone());
        match (reply, p.reply) {
            (Some(r), Some(tx)) => {
                let _ = tx.send(r);
                Ok(format!("{id} answered for {address}"))
            }
            _ => Ok(format!("{id} rejected for {address}")),
        }
    }

    pub async fn answer(&self, query: &str, payload: &[u8]) -> Result<Vec<u8>, Error> {
        let _: NoInput = input(query, payload)?;
        let v = match query {
            STATUS => {
                self.bluez()?;
                self.status.lock().unwrap().record()
            }
            ADAPTERS => {
                let a = self.bluez()?.adapters().await?;
                json!({ "schema": SCHEMA, "adapters": a.iter().map(adapter_json).collect::<Vec<_>>() })
            }
            DEVICES => {
                let d = self.bluez()?.devices().await?;
                json!({ "schema": SCHEMA, "devices": d.iter().map(device_json).collect::<Vec<_>>() })
            }
            other => return Err(Error::Invalid(format!("{other} is not a question of {PLUGIN}"))),
        };
        Ok(v.to_string().into_bytes())
    }

    /// Carries a command out, and returns the line it is acknowledged with. No line holds a PIN.
    pub async fn act(&self, command: &str, payload: &[u8]) -> Result<String, Error> {
        match command {
            POWER => {
                let i: PowerInput = input(command, payload)?;
                let adapter = self.bluez()?.set_powered(i.adapter.as_deref(), i.on).await?;
                Ok(format!("{adapter} powered {}", if i.on { "on" } else { "off" }))
            }
            PAIR => {
                let i: AddressInput = input(command, payload)?;
                let a = address(&i.address)?;
                self.bluez()?.pair(&a).await?;
                Ok(format!("paired with {a}"))
            }
            CONNECT => {
                let i: AddressInput = input(command, payload)?;
                let a = address(&i.address)?;
                self.bluez()?.connect(&a).await?;
                Ok(format!("connected to {a}"))
            }
            DISCONNECT => {
                let i: AddressInput = input(command, payload)?;
                let a = address(&i.address)?;
                self.bluez()?.disconnect(&a).await?;
                Ok(format!("disconnected from {a}"))
            }
            TRUST => {
                let i: TrustInput = input(command, payload)?;
                let a = address(&i.address)?;
                self.bluez()?.set_trusted(&a, i.trusted).await?;
                Ok(format!("{a} is {}trusted", if i.trusted { "" } else { "not " }))
            }
            FORGET => {
                let i: AddressInput = input(command, payload)?;
                let a = address(&i.address)?;
                self.bluez()?.forget(&a).await?;
                Ok(format!("{a} forgotten"))
            }
            ANSWER => {
                let i: AnswerInput = input(command, payload)?;
                self.answer_pairing(i)
            }
            other => Err(Error::Invalid(format!("{other} is not a command of {PLUGIN}"))),
        }
    }
}

pub async fn handle(u: &impl Acts, event: &Event, bt: &Arc<Bt>) -> Result<(), yoke_sdk::base::Error> {
    match event {
        Event::Question(q) if QUERIES.contains(&q.r#type.as_str()) => match bt.answer(&q.r#type, &q.payload).await {
            Ok(answer) => u.answer(q, answer).await,
            Err(e) => u.fail(q, code(&e), &message(&e)).await,
        },
        Event::Command(c) if COMMANDS.contains(&c.r#type.as_str()) => {
            let job = bt.act(&c.r#type, &c.payload);
            tokio::pin!(job);
            let first = match tokio::time::timeout(COMMAND_WAIT, &mut job).await {
                Ok(Ok(line)) => u.ack(c, Outcome::Done, &line).await,
                Ok(Err(e)) => u.ack(c, Outcome::Failed, &format!("{}: {}", code(&e), message(&e))).await,
                Err(_) => {
                    let line = format!("still running after {} s: {JOB_FINISHED} says how it ends", COMMAND_WAIT.as_secs());
                    u.ack(c, Outcome::Accepted, &line).await?;
                    let (line, severity) = match job.await {
                        Ok(line) => (line, 10),
                        Err(e) => (format!("{}: {}", code(&e), message(&e)), 50),
                    };
                    let detail = json!({ "schema": SCHEMA, "line": line, "command": c.r#type, "command_id": c.id });
                    u.report(JOB_FINISHED, severity, &line, detail).await
                }
            };
            // An answer changes the status: the request is no longer waiting.
            if c.r#type == ANSWER {
                return first.and(bt.refresh(u).await);
            }
            first
        }
        _ => Ok(()),
    }
}
