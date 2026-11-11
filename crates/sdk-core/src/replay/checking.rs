//! Experimental depth-first exploration of timer, signal, and remote activity histories.

use super::{
    HistoryFeeder, HistoryForReplay, ReplayWorkerInput,
    model::{ActivitySamples, ModelError, ModelEvent, ModelHistory},
};
use crate::{Worker, WorkerConfig, WorkflowTaskCompletion, init_replay_worker};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
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

/// External inputs eligible during exploration.
#[derive(Debug, Default)]
pub struct CheckInputs {
    /// Each configured signal remains eligible at every open frontier.
    pub signals: Vec<WorkflowExecutionSignaledEventAttributes>,
    /// Outcomes shared by invocations of each remote activity type.
    pub activity_samples: BTreeMap<String, ActivitySamples>,
    /// Explore one external workflow cancellation request per branch.
    pub workflow_cancellation: bool,
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
    /// Every configured signal remains eligible at every open frontier. Timer and activity
    /// resolutions are explored before incoming signals, so repeated signals cannot monopolize
    /// the search. Activity implementations are never executed; results come from the supplied
    /// outcome sets. This prototype explores singleton event batches only.
    pub async fn run(
        &mut self,
        workflow_id: &str,
        initial: ModelHistory,
        inputs: &CheckInputs,
        limits: CheckLimits,
    ) -> Result<Option<Counterexample>, CheckError> {
        self.run_with_history_callback(workflow_id, initial, inputs, limits, |_| {})
            .await
    }

    /// Explore while reporting each submitted frontier and each committed task completion.
    ///
    /// Completed histories retain command consequences, including terminal events, so replay can
    /// compare the last completed task rather than only the tasks preceding a frontier. Queued
    /// frontiers that are never evaluated because of a bound or counterexample are not reported.
    /// Histories reported before a model error remain available to the caller.
    pub async fn run_with_history_callback(
        &mut self,
        workflow_id: &str,
        initial: ModelHistory,
        inputs: &CheckInputs,
        limits: CheckLimits,
        mut on_history: impl FnMut(&History) + Send,
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
            on_history(history.history());
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
            on_history(settled.history());
            if settled.is_closed() || depth == limits.max_depth {
                continue;
            }
            let choices = settled
                .timer_choices()
                .into_iter()
                .chain(settled.activity_choices(&inputs.activity_samples)?)
                .chain(
                    (inputs.workflow_cancellation && !settled.workflow_cancellation_requested())
                        .then_some(ModelEvent::CancelWorkflow),
                )
                .chain(inputs.signals.iter().cloned().map(ModelEvent::Signal))
                .collect::<Vec<_>>();
            for choice in choices.into_iter().rev() {
                stack.push((settled.extend(&[choice])?, depth + 1));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::default_wes_attribs;
    use temporalio_common::protos::temporal::api::{
        command::v1::{
            Command, CommandAttributesExt, CompleteWorkflowExecutionCommandAttributes,
            ContinueAsNewWorkflowExecutionCommandAttributes, StartTimerCommandAttributes, command,
        },
        enums::v1::VersioningBehavior,
        history::v1::history_event,
    };

    #[tokio::test]
    async fn reports_frontiers_and_committed_histories_at_bounds_and_on_errors() {
        for mode in ["terminal", "depth_bound", "model_error", "task_failure"] {
            let (feeder, _histories) = HistoryFeeder::new(1);
            let (tx, results) = mpsc::unbounded_channel();
            let evicted = Arc::new(Notify::new());
            let mut checker = WorkflowCheck {
                feeder,
                results,
                evicted: evicted.clone(),
            };
            let initial = ModelHistory::new(
                default_wes_attribs(),
                std::time::SystemTime::UNIX_EPOCH.into(),
            )
            .unwrap();
            let attributes = match mode {
                "depth_bound" => {
                    command::Attributes::StartTimerCommandAttributes(StartTimerCommandAttributes {
                        timer_id: "timer".to_owned(),
                        start_to_fire_timeout: Some(Duration::from_secs(1).try_into().unwrap()),
                    })
                }
                "model_error" => {
                    command::Attributes::ContinueAsNewWorkflowExecutionCommandAttributes(
                        ContinueAsNewWorkflowExecutionCommandAttributes::default(),
                    )
                }
                _ => command::Attributes::CompleteWorkflowExecutionCommandAttributes(
                    CompleteWorkflowExecutionCommandAttributes::default(),
                ),
            };
            let completion = WorkflowTaskCompletion {
                task_token: vec![].into(),
                commands: vec![Command {
                    command_type: attributes.as_type() as i32,
                    attributes: Some(attributes),
                    ..Default::default()
                }],
                messages: vec![],
                sticky_attributes: None,
                query_responses: vec![],
                return_new_workflow_task: false,
                force_create_new_workflow_task: false,
                sdk_metadata: Default::default(),
                metering_metadata: Default::default(),
                versioning_behavior: VersioningBehavior::Unspecified,
                pagination_enabled: false,
                wft_completion_size_limit: None,
            };
            tx.send(if mode == "task_failure" {
                TaskResult::Failed(Failure::default())
            } else {
                TaskResult::Completed(completion)
            })
            .unwrap();
            evicted.notify_one();
            let mut observed = vec![];
            let result = checker
                .run_with_history_callback(
                    "test",
                    initial,
                    &CheckInputs::default(),
                    CheckLimits {
                        max_depth: 0,
                        max_checks: 1,
                        timeout: Duration::from_secs(5),
                        task_timeout: Duration::from_secs(5),
                    },
                    |history| observed.push(history.clone()),
                )
                .await;
            assert!(matches!(
                observed[0].events.last().unwrap().attributes,
                Some(history_event::Attributes::WorkflowTaskStartedEventAttributes(_))
            ));
            match mode {
                "task_failure" => {
                    assert!(result.unwrap().is_some());
                    assert_eq!(observed.len(), 1);
                }
                "model_error" => {
                    assert!(matches!(result, Err(CheckError::Model(_))));
                    assert_eq!(observed.len(), 1);
                }
                _ => {
                    assert!(result.unwrap().is_none());
                    assert_eq!(observed.len(), 2);
                    let last = observed[1].events.last().unwrap().attributes.as_ref();
                    assert!(if mode == "terminal" {
                        matches!(
                            last,
                            Some(history_event::Attributes::WorkflowExecutionCompletedEventAttributes(_))
                        )
                    } else {
                        matches!(
                            last,
                            Some(history_event::Attributes::TimerStartedEventAttributes(_))
                        )
                    });
                }
            }
        }
    }
}
