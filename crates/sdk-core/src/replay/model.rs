//! A history-only model for exploring timers, signals, and remote activity results.
//!
//! Transitions return new histories so callers can backtrack without restoring cached execution
//! state. Search attribute and memo updates and patch markers are recorded without adding choices.
//! Activity failures, timeouts, retries, cancellation, local activities, updates, execution retries,
//! and workflow-task retries are not modeled.

use crate::WorkflowTaskCompletion;
use prost::Message;
use prost_types::Timestamp;
use std::{collections::BTreeMap, time::Duration};
use temporalio_common::protos::{
    constants::PATCH_MARKER_NAME,
    coresdk::common::decode_change_marker_details,
    temporal::api::{
        command::v1::{CommandAttributesExt, command},
        common::v1::Payloads,
        enums::v1::{CommandType, EventType, RetryState, VersioningBehavior},
        history::v1::{
            ActivityTaskCompletedEventAttributes, ActivityTaskScheduledEventAttributes,
            ActivityTaskStartedEventAttributes, History, HistoryEvent,
            MarkerRecordedEventAttributes, TimerCanceledEventAttributes, TimerFiredEventAttributes,
            TimerStartedEventAttributes, UpsertWorkflowSearchAttributesEventAttributes,
            WorkflowExecutionCompletedEventAttributes, WorkflowExecutionFailedEventAttributes,
            WorkflowExecutionSignaledEventAttributes, WorkflowExecutionStartedEventAttributes,
            WorkflowPropertiesModifiedEventAttributes, WorkflowTaskCompletedEventAttributes,
            WorkflowTaskScheduledEventAttributes, WorkflowTaskStartedEventAttributes,
            history_event,
        },
    },
};

#[cfg(test)]
mod tests;

/// An external event to deliver before starting the next workflow task.
#[derive(Clone, Debug, PartialEq)]
pub enum ModelEvent {
    /// Any pending timer may fire; choosing it advances time to at least its deadline.
    FireTimer(String),
    /// Signals remain eligible independently of whether the workflow is awaiting them.
    Signal(WorkflowExecutionSignaledEventAttributes),
    /// Resolve a pending remote activity using a sampled result, without executing its implementation.
    CompleteActivity {
        /// The schedule event identifies this invocation even if its activity ID is reused later.
        scheduled_event_id: i64,
        /// Payloads to record in history and reuse during replay.
        result: Payloads,
    },
}

/// A branch whose only execution state is its history.
#[derive(Clone, Debug)]
pub struct ModelHistory {
    history: History,
}

/// A transition outside the supported model or an invalid choice.
#[derive(Debug, thiserror::Error)]
pub enum ModelError {
    /// The initial history would be missing information needed for replay.
    #[error("invalid workflow start: {0}")]
    InvalidStart(&'static str),
    /// No further tasks can be scheduled after a terminal command.
    #[error("the workflow execution is closed")]
    WorkflowClosed,
    /// External events cannot be delivered until the current task completes.
    #[error("complete the pending workflow task before extending its history")]
    WorkflowTaskPending,
    /// A task cannot be completed twice.
    #[error("the history does not end at an unfinished workflow task")]
    NoWorkflowTask,
    /// An empty batch would create a task without new work.
    #[error("an extension must contain at least one event")]
    EmptyBatch,
    /// The checker cannot yet interpret this part of a task completion.
    #[error("unsupported workflow task completion: {0}")]
    UnsupportedCompletion(&'static str),
    /// Commands outside the prototype must not silently disappear.
    #[error("unsupported workflow command: {0:?}")]
    UnsupportedCommand(CommandType),
    /// The command's type and attributes disagree or attributes are absent.
    #[error("missing or mismatched attributes for command {0:?}")]
    InvalidCommand(CommandType),
    /// Live timer IDs must be unique.
    #[error("timer {0:?} is already pending")]
    DuplicateTimer(String),
    /// A canceled or fired timer cannot resolve again.
    #[error("timer {0:?} is not pending")]
    TimerNotPending(String),
    /// Timer deadlines must be representable protobuf timestamps.
    #[error("timer {0:?} must have a positive timeout and a representable deadline")]
    InvalidTimerTimeout(String),
    /// Live activity IDs must be unique.
    #[error("activity {0:?} is already pending")]
    DuplicateActivity(String),
    /// An activity completion must refer to an unresolved invocation.
    #[error("activity scheduled at event {0} is not pending")]
    ActivityNotPending(i64),
    /// Missing samples must not silently leave an activity unresolved.
    #[error("no sampled results for activity type {0:?}")]
    MissingActivityResults(String),
    /// An unnamed signal cannot be dispatched to a handler.
    #[error("a signal must have a name")]
    EmptySignalName,
}

impl ModelHistory {
    /// Start a single execution, ending at its first unfinished workflow task.
    ///
    /// Callers supply the run identity and time so sibling branches replay the same initial state.
    /// Execution-level timeouts, retries, cron, and delayed starts are not supported.
    pub fn new(
        mut start: WorkflowExecutionStartedEventAttributes,
        time: Timestamp,
    ) -> Result<Self, ModelError> {
        if start
            .workflow_type
            .as_ref()
            .is_none_or(|t| t.name.is_empty())
            || start.task_queue.as_ref().is_none_or(|q| q.name.is_empty())
            || start.original_execution_run_id.is_empty()
        {
            return Err(ModelError::InvalidStart(
                "workflow type, task queue, and original run ID are required",
            ));
        }
        if start
            .workflow_task_timeout
            .and_then(|d| Duration::try_from(d).ok())
            .is_none_or(|d| d.is_zero())
        {
            return Err(ModelError::InvalidStart(
                "workflow task timeout must be positive",
            ));
        }
        if !(-62_135_596_800..=253_402_300_799).contains(&time.seconds)
            || !(0..1_000_000_000).contains(&time.nanos)
        {
            return Err(ModelError::InvalidStart("invalid start timestamp"));
        }
        if start.workflow_execution_timeout.is_some()
            || start.workflow_run_timeout.is_some()
            || start.retry_policy.is_some()
            || !start.cron_schedule.is_empty()
            || start.first_workflow_task_backoff.is_some()
        {
            return Err(ModelError::InvalidStart(
                "execution timeouts, retries, cron, and delayed starts are not modeled",
            ));
        }
        if start.first_execution_run_id.is_empty() {
            start.first_execution_run_id = start.original_execution_run_id.clone();
        }
        start.attempt = 1;
        let mut model = Self {
            history: History::default(),
        };
        model.push(start.into(), time);
        let scheduled = model.schedule_task();
        model.start_task(scheduled);
        Ok(model)
    }

    /// The prefix to feed to Core, including the unfinished task when one is pending.
    pub fn history(&self) -> &History {
        &self.history
    }

    /// Whether a terminal command has closed this branch.
    pub fn is_closed(&self) -> bool {
        self.history
            .events
            .last()
            .unwrap()
            .is_final_wf_execution_event()
    }

    /// Pending timer choices in stable timer-ID order, available only between tasks.
    ///
    /// Signal choices come from the caller's input generator rather than from history.
    pub fn timer_choices(&self) -> Vec<ModelEvent> {
        if self.is_closed() || self.task_pending() {
            return vec![];
        }
        self.pending_timers()
            .into_keys()
            .map(|id| ModelEvent::FireTimer(id.to_owned()))
            .collect()
    }

    /// Pending remote activity resolutions in schedule order, using the caller's sampled payloads.
    pub fn activity_choices(
        &self,
        results: &BTreeMap<String, Vec<Payloads>>,
    ) -> Result<Vec<ModelEvent>, ModelError> {
        if self.is_closed() || self.task_pending() {
            return Ok(vec![]);
        }
        let mut choices = vec![];
        for (scheduled_event_id, activity) in self.pending_activities() {
            let name = &activity.activity_type.as_ref().unwrap().name;
            let samples = results
                .get(name)
                .filter(|samples| !samples.is_empty())
                .ok_or_else(|| ModelError::MissingActivityResults(name.clone()))?;
            choices.extend(
                samples
                    .iter()
                    .cloned()
                    .map(|result| ModelEvent::CompleteActivity {
                        scheduled_event_id,
                        result,
                    }),
            );
        }
        Ok(choices)
    }

    /// Commit the normal Core task completion without modifying the original prefix.
    pub fn complete_task(&self, completion: &WorkflowTaskCompletion) -> Result<Self, ModelError> {
        if self.is_closed() {
            return Err(ModelError::WorkflowClosed);
        }
        let last = self.history.events.last().unwrap();
        let Some(history_event::Attributes::WorkflowTaskStartedEventAttributes(started)) =
            last.attributes.as_ref()
        else {
            return Err(ModelError::NoWorkflowTask);
        };
        if !completion.messages.is_empty()
            || !completion.query_responses.is_empty()
            || completion.force_create_new_workflow_task
            || completion.versioning_behavior != VersioningBehavior::Unspecified
        {
            return Err(ModelError::UnsupportedCompletion(
                "protocol messages, queries, forced tasks, and worker versioning are not modeled",
            ));
        }
        let mut next = self.clone();
        let completed_id = next.push(
            WorkflowTaskCompletedEventAttributes {
                scheduled_event_id: started.scheduled_event_id,
                started_event_id: last.event_id,
                sdk_metadata: Some(completion.sdk_metadata.clone()),
                metering_metadata: Some(completion.metering_metadata),
                ..Default::default()
            }
            .into(),
            next.time(),
        );
        for cmd in &completion.commands {
            if next.is_closed() {
                return Err(ModelError::WorkflowClosed);
            }
            let attrs = cmd
                .attributes
                .as_ref()
                .filter(|a| a.as_type() as i32 == cmd.command_type)
                .ok_or_else(|| ModelError::InvalidCommand(cmd.command_type()))?;
            let event: history_event::Attributes = match attrs {
                command::Attributes::StartTimerCommandAttributes(a) => {
                    if a.timer_id.is_empty() {
                        return Err(ModelError::InvalidCommand(CommandType::StartTimer));
                    }
                    if next.pending_timers().contains_key(a.timer_id.as_str()) {
                        return Err(ModelError::DuplicateTimer(a.timer_id.clone()));
                    }
                    timer_deadline(next.time(), a.start_to_fire_timeout, &a.timer_id)?;
                    TimerStartedEventAttributes {
                        timer_id: a.timer_id.clone(),
                        start_to_fire_timeout: a.start_to_fire_timeout,
                        workflow_task_completed_event_id: completed_id,
                    }
                    .into()
                }
                command::Attributes::CancelTimerCommandAttributes(a) => {
                    let started_id = next
                        .pending_timers()
                        .get(a.timer_id.as_str())
                        .ok_or_else(|| ModelError::TimerNotPending(a.timer_id.clone()))?
                        .event_id;
                    TimerCanceledEventAttributes {
                        timer_id: a.timer_id.clone(),
                        started_event_id: started_id,
                        workflow_task_completed_event_id: completed_id,
                        ..Default::default()
                    }
                    .into()
                }
                command::Attributes::ScheduleActivityTaskCommandAttributes(a) => {
                    if a.activity_id.is_empty()
                        || a.activity_type.as_ref().is_none_or(|t| t.name.is_empty())
                        || a.task_queue.as_ref().is_none_or(|q| q.name.is_empty())
                    {
                        return Err(ModelError::InvalidCommand(
                            CommandType::ScheduleActivityTask,
                        ));
                    }
                    if next
                        .pending_activities()
                        .values()
                        .any(|pending| pending.activity_id == a.activity_id)
                    {
                        return Err(ModelError::DuplicateActivity(a.activity_id.clone()));
                    }
                    ActivityTaskScheduledEventAttributes {
                        activity_id: a.activity_id.clone(),
                        activity_type: a.activity_type.clone(),
                        task_queue: a.task_queue.clone(),
                        header: a.header.clone(),
                        input: a.input.clone(),
                        schedule_to_close_timeout: a.schedule_to_close_timeout,
                        schedule_to_start_timeout: a.schedule_to_start_timeout,
                        start_to_close_timeout: a.start_to_close_timeout,
                        heartbeat_timeout: a.heartbeat_timeout,
                        retry_policy: a.retry_policy.clone(),
                        workflow_task_completed_event_id: completed_id,
                        priority: a.priority.clone(),
                        ..Default::default()
                    }
                    .into()
                }
                command::Attributes::UpsertWorkflowSearchAttributesCommandAttributes(a) => {
                    UpsertWorkflowSearchAttributesEventAttributes {
                        workflow_task_completed_event_id: completed_id,
                        search_attributes: a.search_attributes.clone(),
                    }
                    .into()
                }
                command::Attributes::ModifyWorkflowPropertiesCommandAttributes(a) => {
                    WorkflowPropertiesModifiedEventAttributes {
                        workflow_task_completed_event_id: completed_id,
                        upserted_memo: a.upserted_memo.clone(),
                    }
                    .into()
                }
                command::Attributes::RecordMarkerCommandAttributes(a)
                    if a.marker_name == PATCH_MARKER_NAME =>
                {
                    if decode_change_marker_details(&a.details).is_none() {
                        return Err(ModelError::InvalidCommand(CommandType::RecordMarker));
                    }
                    MarkerRecordedEventAttributes {
                        marker_name: a.marker_name.clone(),
                        details: a.details.clone(),
                        workflow_task_completed_event_id: completed_id,
                        header: a.header.clone(),
                        failure: a.failure.clone(),
                    }
                    .into()
                }
                command::Attributes::CompleteWorkflowExecutionCommandAttributes(a) => {
                    WorkflowExecutionCompletedEventAttributes {
                        result: a.result.clone(),
                        workflow_task_completed_event_id: completed_id,
                        ..Default::default()
                    }
                    .into()
                }
                command::Attributes::FailWorkflowExecutionCommandAttributes(a) => {
                    WorkflowExecutionFailedEventAttributes {
                        failure: a.failure.clone(),
                        retry_state: RetryState::RetryPolicyNotSet as i32,
                        workflow_task_completed_event_id: completed_id,
                        ..Default::default()
                    }
                    .into()
                }
                _ => return Err(ModelError::UnsupportedCommand(cmd.command_type())),
            };
            next.push(event, next.time());
            let event = next.history.events.last_mut().unwrap();
            event.user_metadata = cmd.user_metadata.clone();
            event.event_group_markers = cmd.event_group_markers.clone();
        }
        Ok(next)
    }

    /// Deliver an ordered, nonempty batch and start one workflow task for it.
    ///
    /// All pending timers are eligible, regardless of deadline order. Firing a timer advances the
    /// history timestamp to at least its deadline, without forcing other timers to fire.
    pub fn extend(&self, batch: &[ModelEvent]) -> Result<Self, ModelError> {
        if self.is_closed() {
            return Err(ModelError::WorkflowClosed);
        }
        if self.task_pending() {
            return Err(ModelError::WorkflowTaskPending);
        }
        if batch.is_empty() {
            return Err(ModelError::EmptyBatch);
        }
        let mut next = self.clone();
        let mut scheduled = 0;
        for event in batch {
            match event {
                ModelEvent::FireTimer(id) => {
                    let timers = next.pending_timers();
                    let started = timers
                        .get(id.as_str())
                        .ok_or_else(|| ModelError::TimerNotPending(id.clone()))?;
                    let Some(history_event::Attributes::TimerStartedEventAttributes(attrs)) =
                        started.attributes.as_ref()
                    else {
                        unreachable!()
                    };
                    let deadline = timer_deadline(
                        started.event_time.unwrap(),
                        attrs.start_to_fire_timeout,
                        id,
                    )?;
                    let now = next.time();
                    let time = if (deadline.seconds, deadline.nanos) > (now.seconds, now.nanos) {
                        deadline
                    } else {
                        now
                    };
                    let attrs = TimerFiredEventAttributes {
                        timer_id: id.clone(),
                        started_event_id: started.event_id,
                    };
                    next.push(attrs.into(), time);
                }
                ModelEvent::Signal(attrs) => {
                    if attrs.signal_name.is_empty() {
                        return Err(ModelError::EmptySignalName);
                    }
                    next.push(attrs.clone().into(), next.time());
                }
                ModelEvent::CompleteActivity {
                    scheduled_event_id,
                    result,
                } => {
                    if !next.pending_activities().contains_key(scheduled_event_id) {
                        return Err(ModelError::ActivityNotPending(*scheduled_event_id));
                    }
                    let started_event_id = next.push(
                        ActivityTaskStartedEventAttributes {
                            scheduled_event_id: *scheduled_event_id,
                            attempt: 1,
                            ..Default::default()
                        }
                        .into(),
                        next.time(),
                    );
                    next.push(
                        ActivityTaskCompletedEventAttributes {
                            scheduled_event_id: *scheduled_event_id,
                            started_event_id,
                            result: Some(result.clone()),
                            ..Default::default()
                        }
                        .into(),
                        next.time(),
                    );
                }
            }
            if scheduled == 0 {
                scheduled = next.schedule_task();
            }
        }
        next.start_task(scheduled);
        Ok(next)
    }

    fn pending_timers(&self) -> BTreeMap<&str, &HistoryEvent> {
        let mut timers = BTreeMap::new();
        for event in &self.history.events {
            match event.attributes.as_ref() {
                Some(history_event::Attributes::TimerStartedEventAttributes(a)) => {
                    timers.insert(a.timer_id.as_str(), event);
                }
                Some(history_event::Attributes::TimerFiredEventAttributes(a)) => {
                    timers.remove(a.timer_id.as_str());
                }
                Some(history_event::Attributes::TimerCanceledEventAttributes(a)) => {
                    timers.remove(a.timer_id.as_str());
                }
                _ => {}
            }
        }
        timers
    }

    fn pending_activities(&self) -> BTreeMap<i64, &ActivityTaskScheduledEventAttributes> {
        let mut activities = BTreeMap::new();
        for event in &self.history.events {
            match event.attributes.as_ref() {
                Some(history_event::Attributes::ActivityTaskScheduledEventAttributes(a)) => {
                    activities.insert(event.event_id, a);
                }
                Some(history_event::Attributes::ActivityTaskCompletedEventAttributes(a)) => {
                    activities.remove(&a.scheduled_event_id);
                }
                _ => {}
            }
        }
        activities
    }

    fn task_pending(&self) -> bool {
        self.history.events.last().unwrap().event_type() == EventType::WorkflowTaskStarted
    }

    fn time(&self) -> Timestamp {
        self.history.events.last().unwrap().event_time.unwrap()
    }

    fn push(&mut self, attributes: history_event::Attributes, time: Timestamp) -> i64 {
        let event_id = self.history.events.len() as i64 + 1;
        self.history.events.push(HistoryEvent {
            event_id,
            event_time: Some(time),
            event_type: attributes.event_type() as i32,
            attributes: Some(attributes),
            ..Default::default()
        });
        event_id
    }

    fn schedule_task(&mut self) -> i64 {
        let Some(history_event::Attributes::WorkflowExecutionStartedEventAttributes(start)) =
            self.history.events[0].attributes.as_ref()
        else {
            unreachable!()
        };
        self.push(
            WorkflowTaskScheduledEventAttributes {
                task_queue: start.task_queue.clone(),
                start_to_close_timeout: start.workflow_task_timeout,
                attempt: 1,
            }
            .into(),
            self.time(),
        )
    }

    fn start_task(&mut self, scheduled_event_id: i64) {
        self.push(
            WorkflowTaskStartedEventAttributes {
                scheduled_event_id,
                history_size_bytes: self.history.encoded_len() as i64,
                ..Default::default()
            }
            .into(),
            self.time(),
        );
    }
}

fn timer_deadline(
    start: Timestamp,
    timeout: Option<prost_types::Duration>,
    timer_id: &str,
) -> Result<Timestamp, ModelError> {
    let invalid = || ModelError::InvalidTimerTimeout(timer_id.to_owned());
    let timeout = timeout.ok_or_else(invalid)?;
    if timeout.seconds < 0
        || !(0..1_000_000_000).contains(&timeout.nanos)
        || (timeout.seconds == 0 && timeout.nanos == 0)
    {
        return Err(invalid());
    }
    let nanos = start.nanos + timeout.nanos;
    let seconds = start
        .seconds
        .checked_add(timeout.seconds)
        .and_then(|s| s.checked_add(i64::from(nanos / 1_000_000_000)))
        .filter(|s| *s <= 253_402_300_799)
        .ok_or_else(invalid)?;
    Ok(Timestamp {
        seconds,
        nanos: nanos % 1_000_000_000,
    })
}
