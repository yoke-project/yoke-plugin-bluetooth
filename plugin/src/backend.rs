//! BlueZ over the system bus, and the pairing agent BlueZ asks while a device pairs.
//!
//! [`Bluez`] reads the adapters and the devices from BlueZ's object manager, acts on them, and says when
//! anything under `/org/bluez` changed. Its agent is registered on the same connection, so that BlueZ
//! routes to it the pairings this connection starts; made the default agent, it is asked about every
//! pairing on the host, a remote device's included. Each request reaches the caller as an [`Ask`], and is
//! answered through the sender the ask carries, or rejected when that sender is dropped.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;
use tokio::sync::{broadcast, mpsc, oneshot};
use zbus::Connection;
use zvariant::{OwnedObjectPath, OwnedValue};

const BLUEZ: &str = "org.bluez";
const ADAPTER: &str = "org.bluez.Adapter1";
const DEVICE: &str = "org.bluez.Device1";
const BATTERY: &str = "org.bluez.Battery1";
const AGENT_MANAGER: &str = "org.bluez.AgentManager1";
/// Where the agent is served on this connection.
pub const AGENT_PATH: &str = "/com/yoke/bluetooth/agent";
/// What the agent can do: show a code, and take one or a yes and a no.
const CAPABILITY: &str = "KeyboardDisplay";

#[derive(Debug)]
pub enum BtError {
    /// BlueZ is not on the bus, or there is no adapter.
    Unavailable(String),
    /// No device of that address, no adapter of that name.
    NotFound(String),
    /// The D-Bus policy or polkit said no.
    PermissionDenied,
    /// BlueZ's own refusal: `org.bluez.Error.<name>` and its message.
    Bluez { name: String, message: String },
    Bus(String),
}

impl std::fmt::Display for BtError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BtError::Unavailable(m) => write!(f, "unavailable: {m}"),
            BtError::NotFound(m) => write!(f, "{m}"),
            BtError::PermissionDenied => write!(f, "the account may not do it"),
            BtError::Bluez { name, message } if message.is_empty() => write!(f, "BlueZ: {name}"),
            BtError::Bluez { name, message } => write!(f, "BlueZ: {name}: {message}"),
            BtError::Bus(m) => write!(f, "{m}"),
        }
    }
}

impl From<zbus::Error> for BtError {
    fn from(e: zbus::Error) -> Self {
        if let zbus::Error::MethodError(name, message, _) = &e {
            let name = name.as_str();
            if let Some(short) = name.strip_prefix("org.bluez.Error.") {
                return BtError::Bluez { name: short.into(), message: message.clone().unwrap_or_default() };
            }
            if name.ends_with("AccessDenied") || name.ends_with("NotAuthorized") {
                return BtError::PermissionDenied;
            }
        }
        BtError::Bus(e.to_string())
    }
}

impl From<zbus::fdo::Error> for BtError {
    fn from(e: zbus::fdo::Error) -> Self {
        BtError::from(zbus::Error::from(e))
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Adapter {
    /// BlueZ's name for it: `hci0`.
    pub id: String,
    pub address: String,
    pub alias: String,
    pub powered: bool,
    pub discovering: bool,
    pub pairable: bool,
    pub discoverable: bool,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Device {
    pub address: String,
    /// The adapter it is known through.
    pub adapter: String,
    pub alias: String,
    /// BlueZ's icon name, which says what kind of device it is: `audio-headset`, `input-mouse`.
    pub icon: String,
    pub paired: bool,
    pub trusted: bool,
    pub blocked: bool,
    pub connected: bool,
    /// While it is in range of a discovery.
    pub rssi: Option<i16>,
    /// Where the device reports one.
    pub battery: Option<u8>,
}

/// What the agent was asked.
#[derive(Debug, Clone, PartialEq)]
pub enum Kind {
    /// A PIN code to type on the device's behalf.
    Pin,
    /// A six-digit passkey the device shows.
    Passkey,
    /// Whether the passkey both sides show is the same.
    Confirm { passkey: u32 },
    /// Whether a pairing with no code may go on.
    Authorize,
    /// Whether a device that is not trusted may use a service.
    Service { uuid: String },
    /// A code to show, which the person types on the device: nothing to answer.
    DisplayPin { pin: String },
    DisplayPasskey { passkey: u32, entered: u16 },
}

impl Kind {
    pub fn name(&self) -> &'static str {
        match self {
            Kind::Pin => "pin",
            Kind::Passkey => "passkey",
            Kind::Confirm { .. } => "confirm",
            Kind::Authorize => "authorize",
            Kind::Service { .. } => "service",
            Kind::DisplayPin { .. } => "display-pin",
            Kind::DisplayPasskey { .. } => "display-passkey",
        }
    }

    /// Whether the agent waits for an answer.
    pub fn asks(&self) -> bool {
        !matches!(self, Kind::DisplayPin { .. } | Kind::DisplayPasskey { .. })
    }
}

/// The answer to an [`Ask`]. A dropped sender is a rejection.
#[derive(Debug, Clone, PartialEq)]
pub enum Reply {
    Accept,
    Pin(String),
    Passkey(u32),
}

#[derive(Debug)]
pub enum Ask {
    /// BlueZ asks about a device: answered through `reply`, which is `None` where nothing is answered.
    Request { address: String, kind: Kind, reply: Option<oneshot::Sender<Reply>> },
    /// BlueZ gave up on the request in flight.
    Cancel,
}

#[async_trait]
pub trait BluetoothBackend: Send + Sync + 'static {
    async fn adapters(&self) -> Result<Vec<Adapter>, BtError>;
    async fn devices(&self) -> Result<Vec<Device>, BtError>;
    async fn set_powered(&self, adapter: Option<&str>, on: bool) -> Result<String, BtError>;
    async fn start_discovery(&self) -> Result<(), BtError>;
    async fn stop_discovery(&self) -> Result<(), BtError>;
    async fn pair(&self, address: &str) -> Result<(), BtError>;
    async fn connect(&self, address: &str) -> Result<(), BtError>;
    async fn disconnect(&self, address: &str) -> Result<(), BtError>;
    async fn set_trusted(&self, address: &str, trusted: bool) -> Result<(), BtError>;
    async fn forget(&self, address: &str) -> Result<(), BtError>;
    /// Says that something changed, without saying what: the caller reads again.
    fn subscribe(&self) -> broadcast::Receiver<()>;
}

fn get<T: TryFrom<OwnedValue>>(m: &HashMap<String, OwnedValue>, key: &str) -> Option<T> {
    m.get(key).and_then(|v| v.try_clone().ok()).and_then(|v| T::try_from(v).ok())
}

type Objects = HashMap<OwnedObjectPath, HashMap<String, HashMap<String, OwnedValue>>>;

pub struct Bluez {
    conn: Connection,
    changed: broadcast::Sender<()>,
}

/// The agent BlueZ calls. Each request becomes an [`Ask`]; the call waits for its answer.
struct Agent {
    conn: Connection,
    asks: mpsc::Sender<Ask>,
}

#[derive(Debug, zbus::DBusError)]
#[zbus(prefix = "org.bluez.Error")]
enum AgentError {
    #[zbus(error)]
    ZBus(zbus::Error),
    Rejected(String),
    Canceled(String),
}

impl Agent {
    async fn address(&self, device: &OwnedObjectPath) -> String {
        let props = async {
            let p = zbus::Proxy::new(&self.conn, BLUEZ, device.as_str(), DEVICE).await?;
            p.get_property::<String>("Address").await
        };
        props.await.unwrap_or_else(|_| device.as_str().rsplit('/').next().unwrap_or_default().to_string())
    }

    async fn ask(&self, device: &OwnedObjectPath, kind: Kind) -> Result<Reply, AgentError> {
        let address = self.address(device).await;
        if !kind.asks() {
            let _ = self.asks.send(Ask::Request { address, kind, reply: None }).await;
            return Ok(Reply::Accept);
        }
        let (tx, rx) = oneshot::channel();
        self.asks
            .send(Ask::Request { address, kind, reply: Some(tx) })
            .await
            .map_err(|_| AgentError::Rejected("nobody to ask".into()))?;
        rx.await.map_err(|_| AgentError::Rejected("not accepted".into()))
    }
}

#[zbus::interface(name = "org.bluez.Agent1")]
impl Agent {
    async fn release(&self) {}

    async fn request_pin_code(&self, device: OwnedObjectPath) -> Result<String, AgentError> {
        match self.ask(&device, Kind::Pin).await? {
            Reply::Pin(pin) => Ok(pin),
            _ => Err(AgentError::Rejected("a PIN was asked for".into())),
        }
    }

    async fn display_pin_code(&self, device: OwnedObjectPath, pincode: String) -> Result<(), AgentError> {
        self.ask(&device, Kind::DisplayPin { pin: pincode }).await.map(|_| ())
    }

    async fn request_passkey(&self, device: OwnedObjectPath) -> Result<u32, AgentError> {
        match self.ask(&device, Kind::Passkey).await? {
            Reply::Passkey(p) if p <= 999_999 => Ok(p),
            _ => Err(AgentError::Rejected("a passkey of six digits was asked for".into())),
        }
    }

    async fn display_passkey(&self, device: OwnedObjectPath, passkey: u32, entered: u16) {
        let _ = self.ask(&device, Kind::DisplayPasskey { passkey, entered }).await;
    }

    async fn request_confirmation(&self, device: OwnedObjectPath, passkey: u32) -> Result<(), AgentError> {
        match self.ask(&device, Kind::Confirm { passkey }).await? {
            Reply::Accept => Ok(()),
            _ => Err(AgentError::Rejected("not confirmed".into())),
        }
    }

    async fn request_authorization(&self, device: OwnedObjectPath) -> Result<(), AgentError> {
        match self.ask(&device, Kind::Authorize).await? {
            Reply::Accept => Ok(()),
            _ => Err(AgentError::Rejected("not authorized".into())),
        }
    }

    async fn authorize_service(&self, device: OwnedObjectPath, uuid: String) -> Result<(), AgentError> {
        match self.ask(&device, Kind::Service { uuid }).await? {
            Reply::Accept => Ok(()),
            _ => Err(AgentError::Rejected("not authorized".into())),
        }
    }

    async fn cancel(&self) {
        let _ = self.asks.send(Ask::Cancel).await;
    }
}

impl Bluez {
    /// Finds BlueZ on the system bus, serves the agent and registers it — as the host's default where
    /// `default_agent` says so — and starts listening for changes. The receiver brings what the agent is
    /// asked.
    pub async fn connect(default_agent: bool) -> Result<(Arc<Bluez>, mpsc::Receiver<Ask>), BtError> {
        let conn = Connection::system().await.map_err(|e| BtError::Unavailable(format!("the system bus: {e}")))?;
        let dbus = zbus::fdo::DBusProxy::new(&conn).await?;
        let names: Vec<String> = dbus.list_names().await?.into_iter().map(|n| n.to_string()).collect();
        if !names.iter().any(|n| n == BLUEZ) {
            return Err(BtError::Unavailable("BlueZ is not on the system bus".into()));
        }
        let (asks, asked) = mpsc::channel(8);
        conn.object_server().at(AGENT_PATH, Agent { conn: conn.clone(), asks }).await?;
        let manager = zbus::Proxy::new(&conn, BLUEZ, "/org/bluez", AGENT_MANAGER).await?;
        let path = OwnedObjectPath::try_from(AGENT_PATH).map_err(|e| BtError::Bus(e.to_string()))?;
        manager.call::<_, _, ()>("RegisterAgent", &(&path, CAPABILITY)).await?;
        if default_agent {
            manager.call::<_, _, ()>("RequestDefaultAgent", &(&path,)).await?;
        }
        let (changed, _) = broadcast::channel(16);
        let me = Arc::new(Bluez { conn, changed });
        let (conn, tx) = (me.conn.clone(), me.changed.clone());
        tokio::spawn(async move {
            let _ = listen(conn, tx).await;
        });
        Ok((me, asked))
    }

    async fn objects(&self) -> Result<Objects, BtError> {
        let om = zbus::fdo::ObjectManagerProxy::builder(&self.conn).destination(BLUEZ)?.path("/")?.build().await?;
        let raw = om.get_managed_objects().await?;
        Ok(raw
            .into_iter()
            .map(|(path, ifaces)| {
                let ifaces = ifaces.into_iter().map(|(i, props)| (i.to_string(), props)).collect();
                (path, ifaces)
            })
            .collect())
    }

    /// The object path of a device, by its address.
    async fn device_path(&self, address: &str) -> Result<OwnedObjectPath, BtError> {
        let wanted = address.to_ascii_uppercase();
        for (path, ifaces) in self.objects().await? {
            if let Some(d) = ifaces.get(DEVICE) {
                if get::<String>(d, "Address").as_deref() == Some(wanted.as_str()) {
                    return Ok(path);
                }
            }
        }
        Err(BtError::NotFound(format!("no device {address}: BlueZ does not know it, or it is out of range")))
    }

    /// The adapter of that name, or the first.
    async fn adapter_path(&self, name: Option<&str>) -> Result<OwnedObjectPath, BtError> {
        let mut paths: Vec<OwnedObjectPath> =
            self.objects().await?.into_iter().filter(|(_, i)| i.contains_key(ADAPTER)).map(|(p, _)| p).collect();
        paths.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        match name {
            Some(n) => paths
                .into_iter()
                .find(|p| p.as_str().rsplit('/').next() == Some(n))
                .ok_or_else(|| BtError::NotFound(format!("no adapter {n}"))),
            None => paths.into_iter().next().ok_or_else(|| BtError::Unavailable("this host has no Bluetooth adapter".into())),
        }
    }

    async fn device_call(&self, address: &str, method: &str) -> Result<(), BtError> {
        let path = self.device_path(address).await?;
        let p = zbus::Proxy::new(&self.conn, BLUEZ, path.as_str(), DEVICE).await?;
        p.call::<_, _, ()>(method, &()).await?;
        Ok(())
    }
}

/// Every signal BlueZ sends under its objects — a property changed, an object added or removed — is one
/// "something changed".
async fn listen(conn: Connection, tx: broadcast::Sender<()>) -> zbus::Result<()> {
    let rule = zbus::MatchRule::builder().msg_type(zbus::message::Type::Signal).sender(BLUEZ)?.build();
    let mut stream = zbus::MessageStream::for_match_rule(rule, &conn, None).await?;
    while let Some(msg) = stream.next().await {
        if msg.is_ok() {
            let _ = tx.send(());
        }
    }
    Ok(())
}

#[async_trait]
impl BluetoothBackend for Bluez {
    async fn adapters(&self) -> Result<Vec<Adapter>, BtError> {
        let mut out: Vec<Adapter> = self
            .objects()
            .await?
            .into_iter()
            .filter_map(|(path, ifaces)| {
                let a = ifaces.get(ADAPTER)?;
                Some(Adapter {
                    id: path.as_str().rsplit('/').next().unwrap_or_default().to_string(),
                    address: get(a, "Address").unwrap_or_default(),
                    alias: get(a, "Alias").unwrap_or_default(),
                    powered: get(a, "Powered").unwrap_or(false),
                    discovering: get(a, "Discovering").unwrap_or(false),
                    pairable: get(a, "Pairable").unwrap_or(false),
                    discoverable: get(a, "Discoverable").unwrap_or(false),
                })
            })
            .collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }

    async fn devices(&self) -> Result<Vec<Device>, BtError> {
        let mut out: Vec<Device> = self
            .objects()
            .await?
            .into_iter()
            .filter_map(|(_, ifaces)| {
                let d = ifaces.get(DEVICE)?;
                let adapter: Option<OwnedObjectPath> = get(d, "Adapter");
                Some(Device {
                    address: get(d, "Address").unwrap_or_default(),
                    adapter: adapter.map(|p| p.as_str().rsplit('/').next().unwrap_or_default().to_string()).unwrap_or_default(),
                    alias: get(d, "Alias").unwrap_or_default(),
                    icon: get(d, "Icon").unwrap_or_default(),
                    paired: get(d, "Paired").unwrap_or(false),
                    trusted: get(d, "Trusted").unwrap_or(false),
                    blocked: get(d, "Blocked").unwrap_or(false),
                    connected: get(d, "Connected").unwrap_or(false),
                    rssi: get(d, "RSSI"),
                    battery: ifaces.get(BATTERY).and_then(|b| get(b, "Percentage")),
                })
            })
            .collect();
        out.sort_by(|a, b| a.address.cmp(&b.address));
        Ok(out)
    }

    async fn set_powered(&self, adapter: Option<&str>, on: bool) -> Result<String, BtError> {
        let path = self.adapter_path(adapter).await?;
        let props = zbus::fdo::PropertiesProxy::builder(&self.conn).destination(BLUEZ)?.path(path.as_str())?.build().await?;
        let iface = zbus::names::InterfaceName::try_from(ADAPTER).map_err(|e| BtError::Bus(e.to_string()))?;
        props.set(iface, "Powered", zvariant::Value::from(on)).await?;
        Ok(path.as_str().rsplit('/').next().unwrap_or_default().to_string())
    }

    async fn start_discovery(&self) -> Result<(), BtError> {
        let path = self.adapter_path(None).await?;
        let p = zbus::Proxy::new(&self.conn, BLUEZ, path.as_str(), ADAPTER).await?;
        match p.call::<_, _, ()>("StartDiscovery", &()).await.map_err(BtError::from) {
            // Discovering already, for this connection or another: what was wanted holds.
            Err(BtError::Bluez { name, .. }) if name == "InProgress" => Ok(()),
            other => other,
        }
    }

    async fn stop_discovery(&self) -> Result<(), BtError> {
        let path = self.adapter_path(None).await?;
        let p = zbus::Proxy::new(&self.conn, BLUEZ, path.as_str(), ADAPTER).await?;
        match p.call::<_, _, ()>("StopDiscovery", &()).await.map_err(BtError::from) {
            // This connection was not discovering: nothing of its own to stop.
            Err(BtError::Bluez { name, .. }) if name == "NotReady" || name == "Failed" => Ok(()),
            other => other,
        }
    }

    async fn pair(&self, address: &str) -> Result<(), BtError> {
        self.device_call(address, "Pair").await
    }

    async fn connect(&self, address: &str) -> Result<(), BtError> {
        self.device_call(address, "Connect").await
    }

    async fn disconnect(&self, address: &str) -> Result<(), BtError> {
        self.device_call(address, "Disconnect").await
    }

    async fn set_trusted(&self, address: &str, trusted: bool) -> Result<(), BtError> {
        let path = self.device_path(address).await?;
        let props = zbus::fdo::PropertiesProxy::builder(&self.conn).destination(BLUEZ)?.path(path.as_str())?.build().await?;
        let iface = zbus::names::InterfaceName::try_from(DEVICE).map_err(|e| BtError::Bus(e.to_string()))?;
        props.set(iface, "Trusted", zvariant::Value::from(trusted)).await?;
        Ok(())
    }

    async fn forget(&self, address: &str) -> Result<(), BtError> {
        let device = self.device_path(address).await?;
        // A device's path is under its adapter's.
        let adapter = device.as_str().rsplit_once('/').map(|(a, _)| a.to_string()).unwrap_or_default();
        let p = zbus::Proxy::new(&self.conn, BLUEZ, adapter.as_str(), ADAPTER).await?;
        p.call::<_, _, ()>("RemoveDevice", &(&device,)).await?;
        Ok(())
    }

    fn subscribe(&self) -> broadcast::Receiver<()> {
        self.changed.subscribe()
    }
}
