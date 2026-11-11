use super::*;
use crate::{
    init_replay_worker,
    replay::{
        HistoryFeeder, HistoryForReplay, HistoryInfo, ReplayWorkerInput, default_wes_attribs,
    },
    test_help::{start_timer_cmd, test_worker_cfg},
};
use temporalio_common::protos::{
    coresdk::{
        workflow_activation::{remove_from_cache::EvictionReason, workflow_activation_job},
        workflow_commands,
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        command::v1::{
            CancelTimerCommandAttributes, CompleteWorkflowExecutionCommandAttributes,
            FailWorkflowExecutionCommandAttributes, ScheduleActivityTaskCommandAttributes,
            StartTimerCommandAttributes,
        },
        common::v1::{Payload, Payloads},
        failure::v1::Failure,
        sdk::v1::WorkflowTaskCompletedMetadata,
    },
};

fn initial() -> ModelHistory {
    let start = WorkflowExecutionStartedEventAttributes {
        original_execution_run_id: "model-run".to_owned(),
        input: Some(Payloads {
            payloads: vec![Payload {
                data: b"workflow arguments".to_vec(),
                ..Default::default()
            }],
        }),
        ..default_wes_attribs()
    };
    ModelHistory::new(
        start,
        Timestamp {
            seconds: 100,
            nanos: 900_000_000,
        },
    )
    .unwrap()
}

fn completion(commands: Vec<command::Attributes>) -> WorkflowTaskCompletion {
    WorkflowTaskCompletion {
        task_token: vec![].into(),
        commands: commands.into_iter().map(Into::into).collect(),
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
    }
}

fn timer(id: &str, duration: Duration) -> command::Attributes {
    command::Attributes::StartTimerCommandAttributes(StartTimerCommandAttributes {
        timer_id: id.to_owned(),
        start_to_fire_timeout: Some(duration.try_into().unwrap()),
    })
}

fn signal(name: &str) -> ModelEvent {
    ModelEvent::Signal(WorkflowExecutionSignaledEventAttributes {
        signal_name: name.to_owned(),
        ..Default::default()
    })
}

#[test]
fn commit_preserves_task_links_and_sdk_flags() {
    let mut comp = completion(vec![timer("1", Duration::from_secs(5))]);
    comp.sdk_metadata = WorkflowTaskCompletedMetadata {
        core_used_flags: vec![1, 2],
        lang_used_flags: vec![3],
        ..Default::default()
    };
    let model = initial().complete_task(&comp).unwrap();
    let events = &model.history().events;
    assert_matches!(
        &events[3].attributes,
        Some(history_event::Attributes::WorkflowTaskCompletedEventAttributes(a))
            if a.scheduled_event_id == 2
                && a.started_event_id == 3
                && a.sdk_metadata.as_ref() == Some(&comp.sdk_metadata)
    );
    assert_matches!(
        &events[4].attributes,
        Some(history_event::Attributes::TimerStartedEventAttributes(a))
            if a.workflow_task_completed_event_id == 4 && a.timer_id == "1"
    );
    assert_matches!(
        &events[0].attributes,
        Some(history_event::Attributes::WorkflowExecutionStartedEventAttributes(a))
            if a.input.as_ref().unwrap().payloads[0].data == b"workflow arguments"
    );
    assert_eq!(
        model.timer_choices(),
        vec![ModelEvent::FireTimer("1".to_owned())]
    );
}

#[test]
fn any_timer_can_fire_without_moving_time_backwards() {
    let root = initial()
        .complete_task(&completion(vec![
            timer("long", Duration::new(2, 200_000_000)),
            timer("short", Duration::from_secs(1)),
        ]))
        .unwrap();
    assert_eq!(
        root.timer_choices(),
        vec![
            ModelEvent::FireTimer("long".to_owned()),
            ModelEvent::FireTimer("short".to_owned()),
        ]
    );
    let long_first = root
        .extend(&[ModelEvent::FireTimer("long".to_owned())])
        .unwrap()
        .complete_task(&completion(vec![]))
        .unwrap();
    assert_eq!(
        long_first.time(),
        Timestamp {
            seconds: 103,
            nanos: 100_000_000,
        }
    );
    let both = long_first
        .extend(&[ModelEvent::FireTimer("short".to_owned())])
        .unwrap()
        .complete_task(&completion(vec![]))
        .unwrap();
    assert_eq!(both.time(), long_first.time());
    assert!(both.timer_choices().is_empty());
    let short_first = root
        .extend(&[ModelEvent::FireTimer("short".to_owned())])
        .unwrap();
    assert_eq!(short_first.time().seconds, 101);
}

#[test]
fn canceled_timers_cannot_fire_and_ids_can_be_reused() {
    let root = initial()
        .complete_task(&completion(vec![timer("1", Duration::from_secs(1))]))
        .unwrap();
    let canceled = root
        .extend(&[signal("cancel")])
        .unwrap()
        .complete_task(&completion(vec![
            command::Attributes::CancelTimerCommandAttributes(CancelTimerCommandAttributes {
                timer_id: "1".to_owned(),
            }),
        ]))
        .unwrap();
    assert!(canceled.timer_choices().is_empty());
    assert_matches!(
        canceled.extend(&[ModelEvent::FireTimer("1".to_owned())]),
        Err(ModelError::TimerNotPending(_))
    );
    assert_matches!(
        canceled.history.events.last().unwrap().attributes.as_ref(),
        Some(history_event::Attributes::TimerCanceledEventAttributes(a))
            if a.started_event_id == 5 && a.workflow_task_completed_event_id == 9
    );

    let restarted = canceled
        .extend(&[signal("restart")])
        .unwrap()
        .complete_task(&completion(vec![timer("1", Duration::from_secs(2))]))
        .unwrap();
    let new_timer_id = restarted.history.events.last().unwrap().event_id;
    let fired = restarted
        .extend(&[ModelEvent::FireTimer("1".to_owned())])
        .unwrap();
    assert_matches!(
        fired.history.events[new_timer_id as usize].attributes.as_ref(),
        Some(history_event::Attributes::TimerFiredEventAttributes(a))
            if a.started_event_id == new_timer_id
    );
}

#[test]
fn a_batch_schedules_one_task_and_preserves_event_order() {
    let root = initial()
        .complete_task(&completion(vec![timer("1", Duration::from_secs(1))]))
        .unwrap();
    let branch = root
        .extend(&[
            signal("first"),
            ModelEvent::FireTimer("1".to_owned()),
            signal("last"),
        ])
        .unwrap();
    let new_events = &branch.history.events[root.history.events.len()..];
    assert_eq!(
        new_events
            .iter()
            .map(|e| e.event_type())
            .collect::<Vec<_>>(),
        vec![
            EventType::WorkflowExecutionSignaled,
            EventType::WorkflowTaskScheduled,
            EventType::TimerFired,
            EventType::WorkflowExecutionSignaled,
            EventType::WorkflowTaskStarted,
        ]
    );
    assert_eq!(new_events[1].event_time, Some(root.time()));
    assert_eq!(new_events[4].event_time, Some(branch.time()));
    assert_matches!(
        new_events[4].attributes.as_ref(),
        Some(history_event::Attributes::WorkflowTaskStartedEventAttributes(a))
            if a.scheduled_event_id == new_events[1].event_id
    );
    let info = HistoryInfo::new_from_history(branch.history(), None).unwrap();
    assert_eq!(info.previous_started_event_id(), 3);
    assert_eq!(info.workflow_task_started_event_id(), 10);
}

#[test]
fn completion_and_failure_close_the_execution() {
    let result = Some(Payloads {
        payloads: vec![Payload {
            data: b"result".to_vec(),
            ..Default::default()
        }],
    });
    let failure = Some(Failure {
        message: "workflow failure".to_owned(),
        ..Default::default()
    });
    for attrs in [
        command::Attributes::CompleteWorkflowExecutionCommandAttributes(
            CompleteWorkflowExecutionCommandAttributes {
                result: result.clone(),
            },
        ),
        command::Attributes::FailWorkflowExecutionCommandAttributes(
            FailWorkflowExecutionCommandAttributes {
                failure: failure.clone(),
            },
        ),
    ] {
        let closed = initial().complete_task(&completion(vec![attrs])).unwrap();
        assert_matches!(
            closed.extend(&[signal("late")]),
            Err(ModelError::WorkflowClosed)
        );
        assert_matches!(
            closed.complete_task(&completion(vec![])),
            Err(ModelError::WorkflowClosed)
        );
        assert!(closed.timer_choices().is_empty());
        match closed.history.events.last().unwrap().attributes.as_ref() {
            Some(history_event::Attributes::WorkflowExecutionCompletedEventAttributes(a)) => {
                assert_eq!(a.result, result);
                assert_eq!(a.workflow_task_completed_event_id, 4);
            }
            Some(history_event::Attributes::WorkflowExecutionFailedEventAttributes(a)) => {
                assert_eq!(a.failure, failure);
                assert_eq!(a.workflow_task_completed_event_id, 4);
                assert_eq!(a.retry_state(), RetryState::RetryPolicyNotSet);
            }
            other => panic!("unexpected terminal event: {other:?}"),
        }
    }
}

#[test]
fn invalid_choices_and_unsupported_commands_are_rejected() {
    let initial = initial();
    assert_matches!(
        initial.extend(&[signal("too-early")]),
        Err(ModelError::WorkflowTaskPending)
    );
    let settled = initial.complete_task(&completion(vec![])).unwrap();
    assert_matches!(
        settled.complete_task(&completion(vec![])),
        Err(ModelError::NoWorkflowTask)
    );
    assert_matches!(settled.extend(&[]), Err(ModelError::EmptyBatch));
    assert_matches!(
        settled.extend(&[ModelEvent::FireTimer("missing".to_owned())]),
        Err(ModelError::TimerNotPending(_))
    );
    assert_matches!(
        initial.complete_task(&completion(vec![
            timer("1", Duration::from_secs(1)),
            command::Attributes::ScheduleActivityTaskCommandAttributes(
                ScheduleActivityTaskCommandAttributes::default()
            ),
        ])),
        Err(ModelError::UnsupportedCommand(
            CommandType::ScheduleActivityTask
        ))
    );
    assert_matches!(
        initial.complete_task(&completion(vec![
            timer("1", Duration::from_secs(1)),
            timer("1", Duration::from_secs(2)),
        ])),
        Err(ModelError::DuplicateTimer(_))
    );
    assert_matches!(
        initial.complete_task(&completion(vec![
            command::Attributes::CompleteWorkflowExecutionCommandAttributes(
                CompleteWorkflowExecutionCommandAttributes::default()
            ),
            timer("1", Duration::from_secs(1)),
        ])),
        Err(ModelError::WorkflowClosed)
    );
}

#[test]
fn malformed_and_overflowing_timer_timeouts_are_rejected() {
    for timeout in [
        None,
        Some(prost_types::Duration::default()),
        Some(prost_types::Duration {
            seconds: -1,
            nanos: 0,
        }),
        Some(prost_types::Duration {
            seconds: 0,
            nanos: 1_000_000_000,
        }),
        Some(prost_types::Duration {
            seconds: i64::MAX,
            nanos: 0,
        }),
    ] {
        assert_matches!(
            initial().complete_task(&completion(vec![
                command::Attributes::StartTimerCommandAttributes(StartTimerCommandAttributes {
                    timer_id: "1".to_owned(),
                    start_to_fire_timeout: timeout,
                }),
            ])),
            Err(ModelError::InvalidTimerTimeout(_))
        );
    }
}

#[tokio::test]
async fn generated_timer_and_signal_batch_replays_through_core() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let model = initial()
            .complete_task(&completion(vec![timer("1", Duration::from_secs(1))]))
            .unwrap()
            .extend(&[signal("signal"), ModelEvent::FireTimer("1".to_owned())])
            .unwrap()
            .complete_task(&completion(vec![
                command::Attributes::CompleteWorkflowExecutionCommandAttributes(
                    CompleteWorkflowExecutionCommandAttributes::default(),
                ),
            ]))
            .unwrap();
        let (feeder, histories) = HistoryFeeder::new(1);
        feeder
            .feed(HistoryForReplay::new(model.history, "model-workflow"))
            .await
            .unwrap();
        let worker = init_replay_worker(ReplayWorkerInput::new(
            test_worker_cfg().build().unwrap(),
            histories,
        ))
        .unwrap();

        let activation = worker.poll_workflow_activation().await.unwrap();
        assert_matches!(
            activation.jobs[0].variant,
            Some(workflow_activation_job::Variant::InitializeWorkflow(_))
        );
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                activation.run_id,
                start_timer_cmd(1, Duration::from_secs(1)),
            ))
            .await
            .unwrap();

        let activation = worker.poll_workflow_activation().await.unwrap();
        assert_eq!(activation.jobs.len(), 2);
        assert_matches!(
            activation.jobs[0].variant.as_ref(),
            Some(workflow_activation_job::Variant::SignalWorkflow(a)) if a.signal_name == "signal"
        );
        assert_matches!(
            activation.jobs[1].variant.as_ref(),
            Some(workflow_activation_job::Variant::FireTimer(a)) if a.seq == 1
        );
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                activation.run_id,
                workflow_commands::CompleteWorkflowExecution::default().into(),
            ))
            .await
            .unwrap();

        let activation = worker.poll_workflow_activation().await.unwrap();
        assert_matches!(
            activation.jobs[0].variant.as_ref(),
            Some(workflow_activation_job::Variant::RemoveFromCache(a))
                if a.reason() == EvictionReason::LangRequested
        );
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::empty(activation.run_id))
            .await
            .unwrap();
        drop(feeder);
        worker.shutdown().await;
        worker.finalize_shutdown().await;
    })
    .await
    .expect("generated history replay did not finish");
}
