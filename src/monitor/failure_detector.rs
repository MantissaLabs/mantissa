use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::time;
use uuid::Uuid;

/// Quality of Service parameters for the failure detector
#[derive(Debug, Clone, Copy)]
pub struct QoSParameters {
    /// Upper bound on the detection time (seconds)
    pub detection_time: f64,
    /// Upper bound on the mistake duration (seconds)
    pub mistake_duration: f64,
    /// Upper bound on the average mistake rate (mistakes per second)
    pub mistake_rate: f64,
}

/// Message types used in the 2W-FD algorithm
#[derive(Debug, Clone)]
pub enum Message {
    /// Heartbeat message from a process to the failure detector
    Heartbeat {
        from: String,
        sequence: u64,
        timestamp: u64,
    },
    /// Query about a process's status
    Query { process_id: String },
    /// Response to a query
    Response { process_id: String, alive: bool },
}

/// Status of a monitored process
#[derive(Debug, Clone, PartialEq)]
pub enum ProcessStatus {
    Alive,
    Suspected,
}

/// Represents a monitored process in the failure detector
#[derive(Debug)]
struct MonitoredProcess {
    id: String,
    last_heartbeat: Instant,
    timeout: Duration,
    status: ProcessStatus,
    freshness_point: Duration,
    sequence: u64,
}

/// The 2W-FD failure detector implementation
#[derive(Clone)]
pub struct TwoWFD {
    /// Unique identifier for this failure detector
    id: String,
    /// Map of processes being monitored
    processes: Arc<Mutex<HashMap<String, MonitoredProcess>>>,
    /// Channel to send messages to the transport layer
    message_sender: mpsc::Sender<(String, Message)>,
    /// QoS parameters
    qos: QoSParameters,
    /// Set of suspected processes
    suspected: Arc<Mutex<HashSet<String>>>,
}

impl TwoWFD {
    /// Create a new 2W-FD failure detector
    pub fn new(qos: QoSParameters, message_sender: mpsc::Sender<(String, Message)>) -> Self {
        TwoWFD {
            id: Uuid::new_v4().to_string(),
            processes: Arc::new(Mutex::new(HashMap::new())),
            message_sender,
            qos,
            suspected: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Start monitoring a new process
    pub async fn add_process(&self, process_id: String) -> Result<(), Box<dyn std::error::Error>> {
        let timeout = Self::calculate_timeout(&self.qos);
        let freshness_point = Self::calculate_freshness_point(&self.qos);

        println!(
            "Adding process {} with timeout {:?}, freshness point {:?}",
            process_id, timeout, freshness_point
        );

        let process = MonitoredProcess {
            id: process_id.clone(),
            last_heartbeat: Instant::now(),
            timeout,
            status: ProcessStatus::Alive,
            freshness_point,
            sequence: 0,
        };

        let mut processes = self.processes.lock().unwrap();
        processes.insert(process_id, process);

        Ok(())
    }

    /// Remove a process from monitoring
    pub async fn remove_process(&self, process_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut processes = self.processes.lock().unwrap();
        processes.remove(process_id);

        let mut suspected = self.suspected.lock().unwrap();
        suspected.remove(process_id);

        Ok(())
    }

    /// Handle incoming messages
    pub async fn handle_message(
        &self,
        from: String,
        message: Message,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match message {
            Message::Heartbeat {
                from,
                sequence,
                timestamp: _,
            } => {
                self.handle_heartbeat(from, sequence).await?;
            }
            Message::Query { process_id } => {
                self.handle_query(from, process_id).await?;
            }
            _ => {
                // Ignore other message types
            }
        }

        Ok(())
    }

    /// Handle a heartbeat message from a process
    async fn handle_heartbeat(
        &self,
        from: String,
        sequence: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut processes = self.processes.lock().unwrap();

        if let Some(process) = processes.get_mut(&from) {
            // Only update if the sequence number is greater
            if sequence > process.sequence {
                process.last_heartbeat = Instant::now();
                process.sequence = sequence;

                // If process was suspected, mark it as alive again
                if process.status == ProcessStatus::Suspected {
                    process.status = ProcessStatus::Alive;

                    let mut suspected = self.suspected.lock().unwrap();
                    suspected.remove(&from);

                    println!("Process {} is now considered ALIVE", from);
                }
            }
        }

        Ok(())
    }

    /// Handle a query about a process's status
    async fn handle_query(
        &self,
        from: String,
        process_id: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let alive = {
            let processes = self.processes.lock().unwrap();
            if let Some(process) = processes.get(&process_id) {
                process.status == ProcessStatus::Alive
            } else {
                false
            }
        };

        let response = Message::Response { process_id, alive };

        self.message_sender.send((from, response)).await?;
        Ok(())
    }

    /// Start the failure detector monitoring loop
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let processes = self.processes.clone();
        let suspected = self.suspected.clone();

        tokio::spawn(async move {
            loop {
                time::sleep(Duration::from_millis(100)).await;

                let now = Instant::now();
                let mut to_suspect = Vec::new();

                // Check all processes
                let mut processes_guard = processes.lock().unwrap();

                for (id, process) in processes_guard.iter_mut() {
                    let elapsed = now.duration_since(process.last_heartbeat);

                    // If timeout has elapsed and process is not already suspected
                    if elapsed > process.timeout && process.status == ProcessStatus::Alive {
                        process.status = ProcessStatus::Suspected;
                        to_suspect.push(id.clone());
                        println!(
                            "Process {} is now SUSPECTED (no heartbeat for {:?})",
                            id, elapsed
                        );
                    }
                }

                // Update the suspected set
                if !to_suspect.is_empty() {
                    let mut suspected_guard = suspected.lock().unwrap();
                    for id in to_suspect {
                        suspected_guard.insert(id);
                    }
                }
            }
        });

        Ok(())
    }

    /// Send a heartbeat to notify other failure detectors about this process
    pub async fn send_heartbeat(&self, target: String) -> Result<(), Box<dyn std::error::Error>> {
        let heartbeat = Message::Heartbeat {
            from: self.id.clone(),
            sequence: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis() as u64,
        };

        self.message_sender.send((target, heartbeat)).await?;
        Ok(())
    }

    /// Query if a process is alive
    pub async fn query_process(
        &self,
        target: String,
        process_id: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let query = Message::Query { process_id };

        self.message_sender.send((target, query)).await?;
        Ok(())
    }

    /// Calculate timeout based on QoS parameters
    fn calculate_timeout(qos: &QoSParameters) -> Duration {
        // Implementation based on the paper's algorithm
        // For simplicity, we're using a basic approach here
        let timeout_secs = qos.detection_time / 2.0;
        Duration::from_secs_f64(timeout_secs)
    }

    /// Calculate freshness point based on QoS parameters
    fn calculate_freshness_point(qos: &QoSParameters) -> Duration {
        // Implementation based on the paper's algorithm
        let fp_secs = qos.mistake_duration / 2.0;
        Duration::from_secs_f64(fp_secs)
    }

    /// Check if a process is suspected
    pub fn is_suspected(&self, process_id: &str) -> bool {
        let suspected = self.suspected.lock().unwrap();
        suspected.contains(process_id)
    }
}

/// Transport abstraction to integrate with different RPC mechanisms
pub trait TransportAdapter {
    fn send_message(
        &self,
        target: String,
        message: Message,
    ) -> Result<(), Box<dyn std::error::Error>>;
    fn receive_messages(&self) -> broadcast::Receiver<(std::string::String, Message)>;
}

/// Example transport adapter for testing
pub struct MockTransport {
    sender: mpsc::Sender<(String, Message)>,
    broadcast_sender: broadcast::Sender<(String, Message)>,
}

impl MockTransport {
    pub fn new() -> Self {
        let (sender, mut receiver) = mpsc::channel(100);
        let (broadcast_sender, _) = broadcast::channel(100);

        let broadcast_sender_clone = broadcast_sender.clone();

        // Forward messages from MPSC to broadcast
        tokio::spawn(async move {
            while let Some(msg) = receiver.recv().await {
                // Ignore errors when no receivers are listening
                let _ = broadcast_sender_clone.send(msg);
            }
        });

        MockTransport {
            sender,
            broadcast_sender,
        }
    }
}

impl TransportAdapter for MockTransport {
    fn send_message(
        &self,
        target: String,
        message: Message,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let sender = self.sender.clone();
        tokio::spawn(async move {
            if let Err(e) = sender.send((target, message)).await {
                eprintln!("Failed to send message: {}", e);
            }
        });
        Ok(())
    }

    fn receive_messages(&self) -> broadcast::Receiver<(std::string::String, Message)> {
        self.broadcast_sender.subscribe()
    }
}

// Example of how to use the implementation with a simple transport layer
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a transport adapter
    let (sender, _receiver) = mpsc::channel(100);

    // Set up QoS parameters
    let qos = QoSParameters {
        detection_time: 5.0,   // 5 seconds
        mistake_duration: 1.0, // 1 second
        mistake_rate: 0.01,    // 0.01 mistakes per second
    };

    // Create the failure detector
    let fd = TwoWFD::new(qos, sender);

    // Start monitoring processes
    fd.add_process("node1".to_string()).await?;
    fd.add_process("node2".to_string()).await?;

    // Start the failure detector
    fd.start().await?;

    // Example: send heartbeats periodically
    let fd_clone = fd.clone();
    tokio::spawn(async move {
        loop {
            time::sleep(Duration::from_secs(1)).await;
            if let Err(e) = fd_clone.send_heartbeat("node1".to_string()).await {
                eprintln!("Failed to send heartbeat: {}", e);
            }
        }
    });

    // Keep the main thread running
    loop {
        time::sleep(Duration::from_secs(1)).await;
    }
}
