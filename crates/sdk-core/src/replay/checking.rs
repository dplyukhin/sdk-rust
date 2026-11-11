//! Experimental depth-first exploration of timer and signal histories.

use super::{
    HistoryFeeder, HistoryForReplay, ReplayWorkerInput,
    model::{ModelError, ModelEvent, ModelHistory},
};
use crate::{Worker, WorkerConfig, WorkflowTaskCompletion, init_replay_worker};
use std::{sync::Arc, time::Duration};
use temporalio_common::protos::temporal::api::{
    enums::v1::TimeoutType,
    failure::v1::{Failure, TimeoutFailureInfo, failure},
    history::v1::{History, WorkflowExecutionSignaledEventAttributes},
};
use tokio::{
    sync::{Notify, mpsc},
    time::{Instant, sleep_until},
};

pub(crate) enum TaskResult {
    Completed(WorkflowTaskCompletion),
    Failed(Failure),
}

/// Exploration bounds. Depth counts extensions after the initial task; checks count new frontiers.
#[derive(Clone, Copy, Debug)]
pub struct CheckLimits {
    /// Maximum extensions along one branch.
    pub max_depth: usize,
    /// Maximum task frontiers evaluated, including the initial task.
    pub max_checks: usize,
    /// Wall-clock budget, including replay and eviction.
    pub timeout: Duration,
    /// Wall-clock limit for one workflow task, including replay.
    pub task_timeout: Duration,
}

/// The input history of the first failed or timed-out task and its failure.
#[derive(Debug)]
pub struct Counterexample {
    /// Ends at the start of the failing task, so replay executes that task again.
    pub history: History,
    /// The worker's failure, or a start-to-close timeout synthesized by the checker.
    pub failure: Failure,
}

/// An invalid model transition or a worker that stopped before reporting its task.
#[derive(Debug, thiserror::Error)]
pub enum CheckError {
    /// The workflow used behavior outside the model.
    #[error(transparent)]
    Model(#[from] ModelError),
    /// The worker stopped before completing a submitted history.
    #[error("workflow checking worker stopped unexpectedly")]
    WorkerStopped,
    /// Zero limits would never evaluate the supplied execution.
    #[error("check count and timeout durations must be positive")]
    InvalidLimits,
}

/// Drives one worker serially, replaying a fresh history for every frontier.
///
/// The language SDK must notify `evicted` after acknowledging each cache-removal activation. This
/// prevents a sibling branch from observing the abandoned branch's cached workflow instance.
pub struct WorkflowCheck {
    feeder: HistoryFeeder,
    results: mpsc::UnboundedReceiver<TaskResult>,
    evicted: Arc<Notify>,
}

/// Create the normal replay worker with task completions routed to a bounded search.
pub fn init_checking_worker(
    config: WorkerConfig,
) -> Result<(Worker, WorkflowCheck), anyhow::Error> {
    let (feeder, histories) = HistoryFeeder::new(1);
    let (tx, results) = mpsc::unbounded_channel();
    let mut input = ReplayWorkerInput::new(config, histories);
    input.task_results = Some(tx);
    let worker = init_replay_worker(input)?;
    Ok((
        worker,
        WorkflowCheck {
            feeder,
            results,
            evicted: Arc::new(Notify::new()),
        },
    ))
}

impl WorkflowCheck {
    /// Notification to signal after the language SDK has completed eviction.
    pub fn eviction_notifier(&self) -> Arc<Notify> {
        self.evicted.clone()
    }

    /// Stop at the first task failure. `None` means no counterexample was found within the bounds.
    ///
    /// Every configured signal remains eligible at every open frontier. Timer resolutions are
    /// explored before incoming signals, so repeated signals cannot monopolize the search.
    /// This prototype explores singleton event batches only.
    pub async fn run(
        &mut self,
        workflow_id: &str,
        initial: ModelHistory,
        signals: &[WorkflowExecutionSignaledEventAttributes],
        limits: CheckLimits,
    ) -> Result<Option<Counterexample>, CheckError> {
        if limits.max_checks == 0 || limits.timeout.is_zero() || limits.task_timeout.is_zero() {
            return Err(CheckError::InvalidLimits);
        }
        let deadline = Instant::now() + limits.timeout;
        let mut stack = vec![(initial, 0)];
        for _ in 0..limits.max_checks {
            let Some((history, depth)) = stack.pop() else {
                break;
            };
            self.feeder
                .feed(HistoryForReplay::new(
                    history.history().clone(),
                    workflow_id,
                ))
                .await
                .map_err(|_| CheckError::WorkerStopped)?;
            let task_deadline = Instant::now() + limits.task_timeout;
            let result = tokio::select! {
                biased;
                _ = sleep_until(deadline) => return Ok(None),
                _ = sleep_until(task_deadline) => {
                    return Ok(Some(Counterexample {
                        history: history.history().clone(),
                        failure: Failure {
                            message: "Workflow task timed out during workflow checking".to_owned(),
                            failure_info: Some(failure::FailureInfo::TimeoutFailureInfo(
                                TimeoutFailureInfo {
                                    timeout_type: TimeoutType::StartToClose as i32,
                                    ..Default::default()
                                },
                            )),
                            ..Default::default()
                        },
                    }));
                }
                result = self.results.recv() => result.ok_or(CheckError::WorkerStopped)?,
            };
            let completion = match result {
                TaskResult::Failed(failure) => {
                    return Ok(Some(Counterexample {
                        history: history.history().clone(),
                        failure,
                    }));
                }
                TaskResult::Completed(completion) => completion,
            };
            tokio::select! {
                _ = sleep_until(deadline) => return Ok(None),
                _ = self.evicted.notified() => {}
            }
            let settled = history.complete_task(&completion)?;
            if settled.is_closed() || depth == limits.max_depth {
                continue;
            }
            let choices = settled
                .timer_choices()
                .into_iter()
                .chain(signals.iter().cloned().map(ModelEvent::Signal))
                .collect::<Vec<_>>();
            for choice in choices.into_iter().rev() {
                stack.push((settled.extend(&[choice])?, depth + 1));
            }
        }
        Ok(None)
    }
}
