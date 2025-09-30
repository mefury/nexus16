//! Runtime for coordinating authenticated workers with multi-worker support

use crate::environment::Environment;
use crate::events::Event;
use crate::orchestrator::OrchestratorClient;
use crate::workers::authenticated_worker::AuthenticatedWorker;
use crate::workers::core::WorkerConfig;
use ed25519_dalek::SigningKey;
use rand_core::OsRng;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

/// Start multiple authenticated workers with difficulty support
#[allow(clippy::too_many_arguments)]
pub async fn start_authenticated_workers_with_difficulty(
    node_id: u64,
    base_signing_key: SigningKey,
    orchestrator: OrchestratorClient,
    shutdown: broadcast::Receiver<()>,
    environment: Environment,
    client_id: String,
    max_tasks: Option<u32>,
    max_difficulty: Option<crate::nexus_orchestrator::TaskDifficulty>,
    num_workers: usize,
) -> (
    mpsc::Receiver<Event>,
    Vec<JoinHandle<()>>,
    broadcast::Sender<()>,
) {
    let mut config = WorkerConfig::new(environment, client_id);
    config.max_difficulty = max_difficulty;
    let (event_sender, event_receiver) =
        mpsc::channel::<Event>(crate::consts::cli_consts::EVENT_QUEUE_SIZE);

    // Create a separate shutdown sender for max tasks completion
    let (shutdown_sender, _) = broadcast::channel(1);

    let mut all_join_handles = Vec::new();

    for worker_id in 0..num_workers {
        // Generate a unique signing key for each worker
        let signing_key = if worker_id == 0 {
            base_signing_key.clone()
        } else {
            SigningKey::generate(&mut OsRng)
        };

        let worker = AuthenticatedWorker::new(
            worker_id,
            node_id,
            signing_key.clone(),
            orchestrator.clone(),
            config.clone(),
            event_sender.clone(),
            max_tasks,
            shutdown_sender.clone(),
            num_workers, // Pass total worker count for round-robin coordination
        );

        let worker_shutdown = shutdown.resubscribe();
        let join_handles = worker.run(worker_shutdown).await;
        all_join_handles.extend(join_handles);
    }

    (event_receiver, all_join_handles, shutdown_sender)
}

/// Start multiple authenticated workers (legacy function for backward compatibility)
#[allow(clippy::too_many_arguments)]
pub async fn start_authenticated_workers(
    node_id: u64,
    base_signing_key: SigningKey,
    orchestrator: OrchestratorClient,
    shutdown: broadcast::Receiver<()>,
    environment: Environment,
    client_id: String,
    max_tasks: Option<u32>,
    num_workers: usize,
) -> (
    mpsc::Receiver<Event>,
    Vec<JoinHandle<()>>,
    broadcast::Sender<()>,
) {
    start_authenticated_workers_with_difficulty(
        node_id,
        base_signing_key,
        orchestrator,
        shutdown,
        environment,
        client_id,
        max_tasks,
        None, // No difficulty override
        num_workers,
    )
    .await
}

/// Start single authenticated worker
#[allow(clippy::too_many_arguments)]
pub async fn start_authenticated_worker(
    node_id: u64,
    signing_key: SigningKey,
    orchestrator: OrchestratorClient,
    shutdown: broadcast::Receiver<()>,
    environment: Environment,
    client_id: String,
    max_tasks: Option<u32>,
    max_difficulty: Option<crate::nexus_orchestrator::TaskDifficulty>,
) -> (
    mpsc::Receiver<Event>,
    Vec<JoinHandle<()>>,
    broadcast::Sender<()>,
) {
    // Delegate to multi-worker function with single worker
    start_authenticated_workers_with_difficulty(
        node_id,
        signing_key,
        orchestrator,
        shutdown,
        environment,
        client_id,
        max_tasks,
        max_difficulty,
        1,
    )
    .await
}
