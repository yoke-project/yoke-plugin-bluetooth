//! The Plugin against a BlueZ that is not there: a fake backend stands in for it — its pairing asks the
//! Plugin as BlueZ asks an agent — and a recorder for the Core.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bluetooth::backend::{Adapter, Ask, BluetoothBackend, BtError, Device, Kind, Reply};
use bluetooth::plugin::*;
use serde_json::{json, Value};
use tokio::sync::{broadcast, mpsc, oneshot};
use yoke_sdk::plugin::{Command, Event, Outcome, Question};

const HEADSET: &str = "94:02:30:00:44:11";
const KEYBOARD: &str = "AA:BB:CC:DD:EE:01";
const SECRET_PIN: &str = "735102";

fn device(address: &str, alias: &str, paired: bool, connected: bool, rssi: Option<i16>) -> Device {
    Device { address: address.into(), adapter: "hci0".into(), alias: alias.into(), paired, connected, rssi, ..Default::default() }
}

/// An adapter with a headset paired and connected, and a keyboard in range. Pairing asks what `asks`
/// says, through `agent`, as BlueZ asks an agent.
struct Fake {
    devices: Mutex<Vec<Device>>,
    discovering: Mutex<bool>,
    agent: mpsc::Sender<Ask>,
    asks: Mutex<Kind>,
    got: Mutex<Option<Reply>>,
    changed: broadcast::Sender<()>,
}

impl Fake {
    fn new() -> (Arc<Fake>, mpsc::Receiver<Ask>) {
        let (agent, asked) = mpsc::channel(4);
        let f = Fake {
            devices: Mutex::new(vec![device(HEADSET, "Headset", true, true, None), device(KEYBOARD, "Keyboard", false, false, Some(-60))]),
            discovering: Mutex::new(false),
            agent,
            asks: Mutex::new(Kind::Confirm { passkey: 42 }),
            got: Mutex::new(None),
            changed: broadcast::channel(4).0,
        };
        (Arc::new(f), asked)
    }
}

#[async_trait]
impl BluetoothBackend for Fake {
    async fn adapters(&self) -> Result<Vec<Adapter>, BtError> {
        Ok(vec![Adapter { id: "hci0".into(), address: "F8:89:D2:15:A2:58".into(), alias: "host".into(), powered: true, discovering: *self.discovering.lock().unwrap(), pairable: true, discoverable: false }])
    }
    async fn devices(&self) -> Result<Vec<Device>, BtError> {
        Ok(self.devices.lock().unwrap().clone())
    }
    async fn set_powered(&self, _adapter: Option<&str>, _on: bool) -> Result<String, BtError> {
        Ok("hci0".into())
    }
    async fn start_discovery(&self) -> Result<(), BtError> {
        *self.discovering.lock().unwrap() = true;
        Ok(())
    }
    async fn stop_discovery(&self) -> Result<(), BtError> {
        *self.discovering.lock().unwrap() = false;
        Ok(())
    }
    async fn pair(&self, address: &str) -> Result<(), BtError> {
        if !self.devices.lock().unwrap().iter().any(|d| d.address == address) {
            return Err(BtError::NotFound(format!("no device {address}")));
        }
        let (tx, rx) = oneshot::channel();
        let kind = self.asks.lock().unwrap().clone();
        self.agent.send(Ask::Request { address: address.into(), kind, reply: Some(tx) }).await.unwrap();
        match rx.await {
            Ok(reply) => {
                *self.got.lock().unwrap() = Some(reply);
                for d in self.devices.lock().unwrap().iter_mut().filter(|d| d.address == address) {
                    d.paired = true;
                }
                Ok(())
            }
            Err(_) => Err(BtError::Bluez { name: "AuthenticationRejected".into(), message: String::new() }),
        }
    }
    async fn connect(&self, _address: &str) -> Result<(), BtError> {
        Ok(())
    }
    async fn disconnect(&self, address: &str) -> Result<(), BtError> {
        for d in self.devices.lock().unwrap().iter_mut().filter(|d| d.address == address) {
            d.connected = false;
        }
        Ok(())
    }
    async fn set_trusted(&self, _address: &str, _trusted: bool) -> Result<(), BtError> {
        Ok(())
    }
    async fn forget(&self, _address: &str) -> Result<(), BtError> {
        Ok(())
    }
    fn subscribe(&self) -> broadcast::Receiver<()> {
        self.changed.subscribe()
    }
}

#[derive(Default)]
struct Core {
    said: Mutex<Vec<String>>,
    details: Mutex<Vec<Value>>,
}

impl Core {
    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.said.lock().unwrap())
    }
    fn push(&self, line: String) -> Result<(), yoke_sdk::base::Error> {
        self.said.lock().unwrap().push(line);
        Ok(())
    }
}

impl Acts for Core {
    async fn answer(&self, _q: &Question, payload: Vec<u8>) -> Result<(), yoke_sdk::base::Error> {
        self.push(format!("answer {}", String::from_utf8_lossy(&payload)))
    }
    async fn fail(&self, _q: &Question, code: &str, message: &str) -> Result<(), yoke_sdk::base::Error> {
        self.push(format!("fail {code}: {message}"))
    }
    async fn ack(&self, _c: &Command, outcome: Outcome, line: &str) -> Result<(), yoke_sdk::base::Error> {
        self.push(format!("ack {outcome:?}: {line}"))
    }
    async fn report(&self, occurrence: &str, severity: u8, line: &str, detail: Value) -> Result<(), yoke_sdk::base::Error> {
        assert_eq!(detail["line"], line, "the detail carries the line a client reads");
        self.details.lock().unwrap().push(detail);
        self.push(format!("report {occurrence} {severity}: {line}"))
    }
    async fn emit(&self, stream: &str, payload: Vec<u8>) -> Result<(), yoke_sdk::base::Error> {
        self.push(format!("emit {stream} {}", String::from_utf8_lossy(&payload)))
    }
}

fn question(r#type: &str) -> Event {
    Event::Question(Question { id: "q-1".into(), r#type: r#type.into(), payload: vec![] })
}

fn command(id: &str, r#type: &str, payload: Value) -> Event {
    Event::Command(Command { id: id.into(), r#type: r#type.into(), payload: payload.to_string().into_bytes() })
}

fn json_of(line: &str, skip: usize) -> Value {
    serde_json::from_str(line.splitn(skip + 1, ' ').nth(skip).unwrap()).unwrap()
}

async fn status(core: &Core, bt: &Arc<Bt>) -> Value {
    handle(core, &question(STATUS), bt).await.unwrap();
    json_of(&core.take().pop().unwrap(), 1)
}

#[test]
fn the_manifest_kept_is_the_manifest_declared() {
    assert_eq!(declaration().manifest(), include_str!("../com.yoke.bluetooth.yaml"));
}

#[test]
fn every_object_declared_has_one_capability() {
    let d = declaration();
    assert_eq!(d.capabilities.len(), d.queries.len() + d.commands.len() + d.streams.len() + d.occurrences.len());
}

#[tokio::test]
async fn a_pairing_asks_the_person_through_the_status_and_an_occurrence_and_waits_for_the_answer() {
    let (fake, mut asked) = Fake::new();
    let core = Arc::new(Core::default());
    let bt = Bt::new(Ok(fake.clone()));
    bt.refresh(core.as_ref()).await.unwrap();
    bt.activated(core.as_ref(), STATUS).await.unwrap();
    core.take();

    // The pairing runs on its own task, as the unit runs every command: it waits for a person.
    let pairing = tokio::spawn({
        let (core, bt) = (core.clone(), bt.clone());
        async move { handle(core.as_ref(), &command("c-1", PAIR, json!({ "address": "aa:bb:cc:dd:ee:01" })), &bt).await.unwrap() }
    });
    let ask = asked.recv().await.unwrap();
    bt.on_ask(core.as_ref(), ask).await.unwrap();
    let said = core.take();
    assert_eq!(said[0], format!("report {PAIRING_REQUEST} 30: {KEYBOARD}: confirm the passkey 000042 (pair-1)"));
    let detail = core.details.lock().unwrap().pop().unwrap();
    assert_eq!((detail["request_id"].as_str(), detail["kind"].as_str(), detail["passkey"].as_str()), (Some("pair-1"), Some("confirm"), Some("000042")));
    // The request is in the status, so a client that attaches now sees it too.
    let emitted = json_of(&said[1], 2);
    assert_eq!(emitted["pairing"]["request_id"], "pair-1");
    assert_eq!(status(&core, &bt).await["pairing"]["alias"], "Keyboard");

    // An answer to another request, or of the wrong shape, leaves this one waiting.
    handle(core.as_ref(), &command("c-2", ANSWER, json!({ "request_id": "pair-9", "accept": true })), &bt).await.unwrap();
    assert_eq!(core.take()[0], "ack Failed: not_found: no pairing request pair-9 waits for an answer");

    handle(core.as_ref(), &command("c-3", ANSWER, json!({ "request_id": "pair-1", "accept": true })), &bt).await.unwrap();
    pairing.await.unwrap();
    let said = core.take();
    assert!(said.contains(&format!("ack Done: pair-1 answered for {KEYBOARD}")), "{said:?}");
    assert!(said.contains(&format!("ack Done: paired with {KEYBOARD}")), "{said:?}");
    assert_eq!(*fake.got.lock().unwrap(), Some(Reply::Accept));
    assert_eq!(status(&core, &bt).await["pairing"], Value::Null);
}

#[tokio::test]
async fn a_pin_is_handed_to_bluez_and_written_nowhere_and_a_refusal_rejects() {
    let (fake, mut asked) = Fake::new();
    *fake.asks.lock().unwrap() = Kind::Pin;
    let core = Arc::new(Core::default());
    let bt = Bt::new(Ok(fake.clone()));
    bt.refresh(core.as_ref()).await.unwrap();

    let pairing = tokio::spawn({
        let (core, bt) = (core.clone(), bt.clone());
        async move { handle(core.as_ref(), &command("c-1", PAIR, json!({ "address": KEYBOARD })), &bt).await.unwrap() }
    });
    bt.on_ask(core.as_ref(), asked.recv().await.unwrap()).await.unwrap();
    // A pin request answered with no pin is not answered.
    handle(core.as_ref(), &command("c-2", ANSWER, json!({ "request_id": "pair-1", "accept": true })), &bt).await.unwrap();
    handle(core.as_ref(), &command("c-3", ANSWER, json!({ "request_id": "pair-1", "accept": true, "pin": SECRET_PIN })), &bt).await.unwrap();
    pairing.await.unwrap();
    assert_eq!(*fake.got.lock().unwrap(), Some(Reply::Pin(SECRET_PIN.into())));
    let said = core.take();
    assert!(said.iter().any(|l| l == "ack Failed: invalid: a pin request is answered with a pin of 1 to 16 characters"), "{said:?}");
    assert!(said.iter().all(|l| !l.contains(SECRET_PIN)), "{said:?}");

    // Refused: BlueZ is told no, and the command that paired says why.
    let pairing = tokio::spawn({
        let (core, bt) = (core.clone(), bt.clone());
        async move { handle(core.as_ref(), &command("c-4", PAIR, json!({ "address": KEYBOARD })), &bt).await.unwrap() }
    });
    bt.on_ask(core.as_ref(), asked.recv().await.unwrap()).await.unwrap();
    handle(core.as_ref(), &command("c-5", ANSWER, json!({ "request_id": "pair-2", "accept": false })), &bt).await.unwrap();
    pairing.await.unwrap();
    let said = core.take();
    assert!(said.contains(&format!("ack Done: pair-2 rejected for {KEYBOARD}")), "{said:?}");
    assert!(said.contains(&"ack Failed: auth_failed: BlueZ: AuthenticationRejected".to_string()), "{said:?}");
}

#[tokio::test]
async fn bluez_cancelling_a_request_takes_it_out_of_the_status() {
    let (fake, _asked) = Fake::new();
    let core = Arc::new(Core::default());
    let bt = Bt::new(Ok(fake.clone()));
    bt.refresh(core.as_ref()).await.unwrap();
    let (tx, rx) = oneshot::channel();
    bt.on_ask(core.as_ref(), Ask::Request { address: KEYBOARD.into(), kind: Kind::Authorize, reply: Some(tx) }).await.unwrap();
    assert_eq!(status(&core, &bt).await["pairing"]["kind"], "authorize");
    bt.on_ask(core.as_ref(), Ask::Cancel).await.unwrap();
    assert_eq!(status(&core, &bt).await["pairing"], Value::Null);
    assert!(rx.await.is_err(), "the agent is answered with a rejection");
}

#[tokio::test]
async fn a_code_to_show_is_reported_and_stays_until_the_device_paired() {
    let (fake, _asked) = Fake::new();
    let core = Arc::new(Core::default());
    let bt = Bt::new(Ok(fake.clone()));
    bt.refresh(core.as_ref()).await.unwrap();
    bt.on_ask(core.as_ref(), Ask::Request { address: KEYBOARD.into(), kind: Kind::DisplayPasskey { passkey: 7, entered: 0 }, reply: None }).await.unwrap();
    assert_eq!(core.take()[0], format!("report {PAIRING_DISPLAY} 30: type the passkey shown on {KEYBOARD}"));
    let s = status(&core, &bt).await;
    assert_eq!((s["pairing"]["passkey"].as_str(), s["pairing"]["answers"].as_bool()), (Some("000007"), Some(false)));
    fake.devices.lock().unwrap()[1].paired = true;
    bt.refresh(core.as_ref()).await.unwrap();
    assert_eq!(status(&core, &bt).await["pairing"], Value::Null);
}

#[tokio::test]
async fn a_device_connecting_and_disconnecting_is_reported() {
    let (fake, _asked) = Fake::new();
    let core = Arc::new(Core::default());
    let bt = Bt::new(Ok(fake.clone()));
    bt.refresh(core.as_ref()).await.unwrap();
    assert!(core.take().is_empty(), "the first reading reports nothing");
    handle(core.as_ref(), &command("c-1", DISCONNECT, json!({ "address": HEADSET })), &bt).await.unwrap();
    bt.refresh(core.as_ref()).await.unwrap();
    assert_eq!(core.take(), vec![format!("ack Done: disconnected from {HEADSET}"), format!("report {DISCONNECTED} 10: Headset ({HEADSET}) disconnected")]);
    fake.devices.lock().unwrap()[0].connected = true;
    bt.refresh(core.as_ref()).await.unwrap();
    assert_eq!(core.take(), vec![format!("report {CONNECTED} 10: Headset ({HEADSET}) connected")]);
}

#[tokio::test]
async fn discovery_runs_while_the_scan_flows_and_shows_what_is_not_paired() {
    let (fake, _asked) = Fake::new();
    let core = Arc::new(Core::default());
    let bt = Bt::new(Ok(fake.clone()));
    bt.activated(core.as_ref(), SCAN).await.unwrap();
    assert!(*fake.discovering.lock().unwrap());
    let scan = json_of(&core.take()[0], 2);
    assert_eq!(scan["devices"].as_array().unwrap().len(), 1);
    assert_eq!(scan["devices"][0]["address"], KEYBOARD);
    bt.stopped(SCAN).await;
    assert!(!*fake.discovering.lock().unwrap());
}

#[tokio::test]
async fn an_address_is_checked_and_a_device_unknown_is_not_found() {
    let (fake, _asked) = Fake::new();
    let core = Arc::new(Core::default());
    let bt = Bt::new(Ok(fake.clone()));
    handle(core.as_ref(), &command("c-1", CONNECT, json!({ "address": "headset" })), &bt).await.unwrap();
    handle(core.as_ref(), &command("c-2", PAIR, json!({ "address": "00:00:00:00:00:00" })), &bt).await.unwrap();
    let said = core.take();
    assert!(said[0].starts_with("ack Failed: invalid: an address is six pairs"), "{said:?}");
    assert_eq!(said[1], "ack Failed: not_found: no device 00:00:00:00:00:00");
}

#[tokio::test]
async fn without_bluez_every_question_and_command_is_failed_with_the_reason() {
    let core = Core::default();
    let bt = Bt::new(Err(BtError::Unavailable("BlueZ is not on the system bus".into())));
    bt.refresh(&core).await.unwrap();
    handle(&core, &question(DEVICES), &bt).await.unwrap();
    handle(&core, &command("c-1", POWER, json!({ "on": true })), &bt).await.unwrap();
    let said = core.take();
    assert!(said[0].starts_with("fail unavailable: "), "{said:?}");
    assert!(said[1].starts_with("ack Failed: unavailable: "), "{said:?}");
}
