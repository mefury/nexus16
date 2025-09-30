//! Single authenticated worker that orchestrates fetch→prove→submit

use super::core::{EventSender, WorkerConfig};
use super::fetcher::TaskFetcher;
use super::prover::TaskProver;
use super::submitter::ProofSubmitter;
use crate::events::{Event, ProverState};
use crate::orchestrator::OrchestratorClient;

use ed25519_dalek::SigningKey;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

/// Stagger delay between workers in milliseconds to reduce thundering herd effect
const WORKER_STAGGER_DELAY_MS: u64 = 200;

/// Authenticated worker that orchestrates fetch→prove→submit
pub struct AuthenticatedWorker {
    worker_id: usize,
    fetcher: TaskFetcher,
    prover: TaskProver,
    submitter: ProofSubmitter,
    event_sender: EventSender,
    max_tasks: Option<u32>,
    tasks_completed: u32,
    shutdown_sender: broadcast::Sender<()>,
}

impl AuthenticatedWorker {
    pub fn new(
        worker_id: usize,
        node_id: u64,
        signing_key: SigningKey,
        orchestrator: OrchestratorClient,
        config: WorkerConfig,
        event_sender: mpsc::Sender<Event>,
        max_tasks: Option<u32>,
        shutdown_sender: broadcast::Sender<()>,
        total_workers: usize,
    ) -> Self {
        let event_sender_helper = EventSender::new(event_sender);

        // Create the 3 specialized components
        let fetcher = TaskFetcher::new(
            node_id,
            signing_key.verifying_key(),
            Box::new(orchestrator.clone()),
            event_sender_helper.clone(),
            &config,
            worker_id,
            total_workers,
        );

        let prover = TaskProver::new(worker_id, event_sender_helper.clone(), config.clone());

        let submitter = ProofSubmitter::new(
            signing_key,
            Box::new(orchestrator),
            event_sender_helper.clone(),
            &config,
        );

        Self {
            worker_id,
            fetcher,
            prover,
            submitter,
            event_sender: event_sender_helper,
            max_tasks,
            tasks_completed: 0,
            shutdown_sender,
        }
    }

    /// Start the worker
    pub async fn run(mut self, mut shutdown: broadcast::Receiver<()>) -> Vec<JoinHandle<()>> {
        let mut join_handles = Vec::new();

        // Implement staggered fetching to reduce thundering herd effect
        // Add delay based on worker ID to spread out initial task fetching
        if self.worker_id > 0 {
            let stagger_delay = std::time::Duration::from_millis(self.worker_id as u64 * WORKER_STAGGER_DELAY_MS);
            self.event_sender
                .send_event(Event::state_change(
                    ProverState::Waiting,
                    format!("Worker {} staggering startup by {}ms", self.worker_id, stagger_delay.as_millis()),
                ))
                .await;

            tokio::select! {
                _ = tokio::time::sleep(stagger_delay) => {},
                _ = shutdown.recv() => {
                    // If shutdown signal received during stagger delay, exit early
                    return vec![];
                }
            }
        }

        // Send initial state
        self.event_sender
            .send_event(Event::state_change(
                ProverState::Waiting,
                format!("Worker {} ready to fetch tasks", self.worker_id),
            ))
            .await;

        // Main work loop
        let worker_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.recv() => break,
                    should_exit = self.work_cycle() => {
                        if should_exit {
                            break;
                        }
                        // Natural rate limiting through work cycle
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        });
        join_handles.push(worker_handle);

        join_handles
    }

    /// Complete work cycle: fetch→prove→submit
    /// Returns true if the worker should exit (max tasks reached)
    async fn work_cycle(&mut self) -> bool {
        // Step 1: Fetch task
        let task = match self.fetcher.fetch_task().await {
            Ok(task) => task,
            Err(_) => {
                // Error already logged in fetcher, wait before retry
                tokio::time::sleep(Duration::from_secs(1)).await;
                return false; // Don't exit on fetch error, just retry
            }
        };

        // Time starts from successfully obtaining the task
        let start_time = std::time::Instant::now();

        // Step 2: Prove task
        // Send state change to Proving
        self.event_sender
            .send_event(Event::state_change(
                ProverState::Proving,
                format!("Step 2 of 4: Proving task {}", task.task_id),
            ))
            .await;

        let proof_result = match self.prover.prove_task(&task).await {
            Ok(proof_result) => proof_result,
            Err(_) => {
                // Send state change back to Waiting on proof failure
                self.event_sender
                    .send_event(Event::state_change(
                        ProverState::Waiting,
                        "Proof generation failed, ready for next task".to_string(),
                    ))
                    .await;
                return false; // Don't exit on proof error, just retry
            }
        };

        // Step 3: Submit proof
        let submission_result = self.submitter.submit_proof(&task, &proof_result).await;

        // Only increment task counter on successful submission
        if submission_result.is_ok() {
            self.tasks_completed += 1;

            // Update success tracking for difficulty promotion
            let duration_secs = start_time.elapsed().as_secs();
            self.fetcher.update_success_tracking(duration_secs);

            // Send information about completing the task
            self.event_sender
                .send_event(Event::state_change(
                    ProverState::Waiting,
                    format!(
                        "{} completed, Task size: {}, Duration: {}s, Difficulty: {}",
                        task.task_id,
                        task.public_inputs_list.len(),
                        self.fetcher.last_success_duration_secs.unwrap_or(0),
                        self.fetcher
                            .last_success_difficulty
                            .map(|difficulty| difficulty.as_str_name())
                            .unwrap_or("Unknown")
                    ),
                ))
                .await;
            // Check if we've reached the maximum number of tasks
            if let Some(max) = self.max_tasks {
                if self.tasks_completed >= max {
                    // Give a brief moment for the "Step 4 of 4" message to be processed
                    // before triggering shutdown
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                    self.event_sender
                        .send_event(Event::state_change(
                            ProverState::Waiting,
                            format!("Completed {} tasks, shutting down", self.tasks_completed),
                        ))
                        .await;

                    // Send shutdown signal to trigger application exit
                    let _ = self.shutdown_sender.send(());
                    return true; // Signal to exit the worker loop
                }
            }
        }

        // Send state change back to Waiting at the end of the work cycle
        self.event_sender
            .send_event(Event::state_change(
                ProverState::Waiting,
                "Task completed, ready for next task".to_string(),
            ))
            .await;

        false // Continue with more tasks
    }
}
