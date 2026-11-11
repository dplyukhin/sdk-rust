use super::*;
use crate::{
    init_replay_worker,
    replay::{
        HistoryFeeder, HistoryForReplay, HistoryInfo, ReplayWorkerInput, default_wes_attribs,
    },
    test_help::{start_timer_cmd, test_worker_cfg},
};
use temporalio_common::protos::{
    constants::LOCAL_ACTIVITY_MARKER_NAME,
    coresdk::{
        activity_result::activity_resolution,
        common::build_has_change_marker_details,
        workflow_activation::{remove_from_cache::EvictionReason, workflow_activation_job},
        workflow_commands,
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        command::v1::{
            CancelTimerCommandAttributes, CancelWorkflowExecutionCommandAttributes, Command,
            CompleteWorkflowExecutionCommandAttributes, FailWorkflowExecutionCommandAttributes,
            ModifyWorkflowPropertiesCommandAttributes, RecordMarkerCommandAttributes,
            RequestCancelActivityTaskCommandAttributes, ScheduleActivityTaskCommandAttributes,
            StartTimerCommandAttributes, UpsertWorkflowSearchAttributesCommandAttributes,
        },
        common::v1::{ActivityType, Memo, Payload, Payloads, RetryPolicy, SearchAttributes},
        failure::v1::{ApplicationFailureInfo, Failure, TimeoutFailureInfo},
        sdk::v1::WorkflowTaskCompletedMetadata,
        taskqueue::v1::TaskQueue,
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
        commands: commands
            .into_iter()
            .map(|attributes| Command {
                command_type: attributes.as_type() as i32,
                attributes: Some(attributes),
                ..Default::default()
            })
            .collect(),
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

fn activity(id: &str) -> command::Attributes {
    command::Attributes::ScheduleActivityTaskCommandAttributes(
        ScheduleActivityTaskCommandAttributes {
            activity_id: id.to_owned(),
            activity_type: Some(ActivityType {
                name: "model-activity".to_owned(),
            }),
            task_queue: Some(TaskQueue {
                name: "model-queue".to_owned(),
                ..Default::default()
            }),
            start_to_close_timeout: Some(Duration::from_secs(10).try_into().unwrap()),
            ..Default::default()
        },
    )
}

fn payloads(value: &str) -> Payloads {
    Payloads {
        payloads: vec![Payload {
            data: value.as_bytes().to_vec(),
            ..Default::default()
        }],
    }
}

fn cancel_activity(scheduled_event_id: i64) -> command::Attributes {
    command::Attributes::RequestCancelActivityTaskCommandAttributes(
        RequestCancelActivityTaskCommandAttributes { scheduled_event_id },
    )
}

#[test]
fn activity_cancellation_keeps_completion_eligible_until_acknowledged() {
    let root = initial()
        .complete_task(&completion(vec![
            activity("remote"),
            timer("deadline", Duration::from_secs(5)),
        ]))
        .unwrap();
    let samples = BTreeMap::from([(
        "model-activity".to_owned(),
        ActivitySamples {
            results: vec![payloads("completed")],
            failures: vec![Failure {
                failure_info: Some(failure::FailureInfo::ApplicationFailureInfo(
                    ApplicationFailureInfo {
                        non_retryable: true,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            }],
        },
    )]);
    assert_matches!(
        root.extend(&[ModelEvent::CancelActivity {
            scheduled_event_id: 5,
            started: false
        }]),
        Err(ModelError::ActivityCancellationNotRequested(5))
    );
    let requested = root
        .extend(&[signal("cancel")])
        .unwrap()
        .complete_task(&completion(vec![cancel_activity(5)]))
        .unwrap();
    let request = requested.history.events.last().unwrap();
    assert_matches!(
        request.attributes.as_ref(),
        Some(history_event::Attributes::ActivityTaskCancelRequestedEventAttributes(a))
            if a.scheduled_event_id == 5 && a.workflow_task_completed_event_id == request.event_id - 1
    );
    let choices = requested.activity_choices(&samples).unwrap();
    assert_eq!(choices.len(), 4);
    assert_eq!(requested.timer_choices(), root.timer_choices());
    for choice in &choices {
        let branch = requested.extend(std::slice::from_ref(choice)).unwrap();
        if let ModelEvent::CancelActivity { started, .. } = choice {
            let canceled = branch
                .history
                .events
                .iter()
                .find_map(|event| {
                    if let Some(history_event::Attributes::ActivityTaskCanceledEventAttributes(a)) =
                        &event.attributes
                    {
                        Some(a)
                    } else {
                        None
                    }
                })
                .unwrap();
            assert_eq!(canceled.latest_cancel_requested_event_id, request.event_id);
            assert_eq!(canceled.scheduled_event_id, 5);
            assert_eq!(canceled.started_event_id != 0, *started);
        }
        let settled = branch.complete_task(&completion(vec![])).unwrap();
        assert!(
            settled
                .activity_choices(&BTreeMap::new())
                .unwrap()
                .is_empty()
        );
        assert_matches!(
            settled.extend(std::slice::from_ref(choice)),
            Err(ModelError::ActivityNotPending(5))
        );
        let reused = settled
            .extend(&[signal("reuse")])
            .unwrap()
            .complete_task(&completion(vec![activity("remote")]))
            .unwrap();
        assert_eq!(reused.activity_choices(&samples).unwrap().len(), 2);
    }
    assert_eq!(root.activity_choices(&samples).unwrap().len(), 2);
    assert_matches!(
        requested
            .extend(&[signal("cancel-again")])
            .unwrap()
            .complete_task(&completion(vec![cancel_activity(5)])),
        Err(ModelError::ActivityCancellationAlreadyRequested(5))
    );
    assert_matches!(
        root.extend(&[signal("unknown")])
            .unwrap()
            .complete_task(&completion(vec![cancel_activity(999)])),
        Err(ModelError::ActivityNotPending(999))
    );
}

#[test]
fn workflow_cancellation_is_requested_once_and_can_close_the_execution() {
    let root = initial().complete_task(&completion(vec![])).unwrap();
    let requested = root.extend(&[ModelEvent::CancelWorkflow]).unwrap();
    assert!(requested.workflow_cancellation_requested());
    assert!(!root.workflow_cancellation_requested());
    let settled = requested.complete_task(&completion(vec![])).unwrap();
    assert_matches!(
        settled.extend(&[ModelEvent::CancelWorkflow]),
        Err(ModelError::WorkflowCancellationAlreadyRequested)
    );
    let closed = settled
        .extend(&[signal("finish-cleanup")])
        .unwrap()
        .complete_task(&completion(vec![
            command::Attributes::CancelWorkflowExecutionCommandAttributes(
                CancelWorkflowExecutionCommandAttributes {
                    details: Some(payloads("cleanup finished")),
                },
            ),
        ]))
        .unwrap();
    assert!(closed.is_closed());
    let last = closed.history.events.last().unwrap();
    assert_matches!(
        last.attributes.as_ref(),
        Some(history_event::Attributes::WorkflowExecutionCanceledEventAttributes(a))
            if a.workflow_task_completed_event_id == last.event_id - 1
                && a.details == Some(payloads("cleanup finished"))
    );
    assert_matches!(
        closed.extend(&[signal("late")]),
        Err(ModelError::WorkflowClosed)
    );
}

#[tokio::test]
async fn generated_activity_cancellations_replay_through_core() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for started in [false, true] {
            let model = initial()
                .complete_task(&completion(vec![activity("remote")]))
                .unwrap()
                .extend(&[signal("cancel")])
                .unwrap()
                .complete_task(&completion(vec![cancel_activity(5)]))
                .unwrap()
                .extend(&[ModelEvent::CancelActivity {
                    scheduled_event_id: 5,
                    started,
                }])
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
            worker
                .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                    activation.run_id,
                    workflow_commands::ScheduleActivity {
                        seq: 1,
                        activity_id: "remote".to_owned(),
                        activity_type: "model-activity".to_owned(),
                        task_queue: "model-queue".to_owned(),
                        start_to_close_timeout: Some(Duration::from_secs(10).try_into().unwrap()),
                        cancellation_type:
                            workflow_commands::ActivityCancellationType::WaitCancellationCompleted
                                as i32,
                        ..Default::default()
                    }
                    .into(),
                ))
                .await
                .unwrap();
            let activation = worker.poll_workflow_activation().await.unwrap();
            assert_matches!(
                activation.jobs[0].variant.as_ref(),
                Some(workflow_activation_job::Variant::SignalWorkflow(_))
            );
            worker
                .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                    activation.run_id,
                    workflow_commands::RequestCancelActivity { seq: 1 }.into(),
                ))
                .await
                .unwrap();
            let activation = worker.poll_workflow_activation().await.unwrap();
            assert_matches!(
                activation.jobs[0].variant.as_ref(),
                Some(workflow_activation_job::Variant::ResolveActivity(a))
                    if matches!(
                        a.result.as_ref().unwrap().status.as_ref().unwrap(),
                        activity_resolution::Status::Cancelled(_)
                    )
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
                Some(workflow_activation_job::Variant::RemoveFromCache(_))
            );
            worker
                .complete_workflow_activation(WorkflowActivationCompletion::empty(
                    activation.run_id,
                ))
                .await
                .unwrap();
            drop(feeder);
            worker.shutdown().await;
            worker.finalize_shutdown().await;
        }
    })
    .await
    .expect("generated cancellation history replay did not finish");
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
fn metadata_commands_do_not_create_choices_or_mutate_sibling_histories() {
    let root = initial()
        .complete_task(&completion(vec![timer("deadline", Duration::from_secs(5))]))
        .unwrap();
    for deprecated in [false, true] {
        let branch = root.extend(&[signal("update")]).unwrap();
        let completed_id = branch.history.events.last().unwrap().event_id + 1;
        let settled = branch
            .complete_task(&completion(vec![
                command::Attributes::RecordMarkerCommandAttributes(RecordMarkerCommandAttributes {
                    marker_name: PATCH_MARKER_NAME.to_owned(),
                    details: build_has_change_marker_details("metadata-patch", deprecated).unwrap(),
                    ..Default::default()
                }),
                command::Attributes::UpsertWorkflowSearchAttributesCommandAttributes(
                    UpsertWorkflowSearchAttributesCommandAttributes {
                        search_attributes: Some(SearchAttributes {
                            indexed_fields: [(
                                "stage".to_owned(),
                                payloads("approved").payloads.remove(0),
                            )]
                            .into(),
                        }),
                    },
                ),
                command::Attributes::ModifyWorkflowPropertiesCommandAttributes(
                    ModifyWorkflowPropertiesCommandAttributes {
                        upserted_memo: Some(Memo {
                            fields: [("scratch".to_owned(), Payload::default())].into(),
                        }),
                    },
                ),
            ]))
            .unwrap();
        let events = &settled.history.events;
        assert_matches!(
            &events[events.len() - 3].attributes,
            Some(history_event::Attributes::MarkerRecordedEventAttributes(a))
                if a.workflow_task_completed_event_id == completed_id
                    && decode_change_marker_details(&a.details)
                        == Some(("metadata-patch".to_owned(), deprecated))
        );
        assert_matches!(
            &events[events.len() - 2].attributes,
            Some(history_event::Attributes::UpsertWorkflowSearchAttributesEventAttributes(a))
                if a.workflow_task_completed_event_id == completed_id
        );
        assert_matches!(
            &events[events.len() - 1].attributes,
            Some(history_event::Attributes::WorkflowPropertiesModifiedEventAttributes(a))
                if a.workflow_task_completed_event_id == completed_id
                    && a.upserted_memo.as_ref().unwrap().fields["scratch"] == Payload::default()
        );
        assert_eq!(settled.timer_choices(), root.timer_choices());
        assert!(!settled.task_pending());
        assert_eq!(root.history.events.len(), 5);
    }
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
            command::Attributes::RecordMarkerCommandAttributes(RecordMarkerCommandAttributes {
                marker_name: LOCAL_ACTIVITY_MARKER_NAME.to_owned(),
                ..Default::default()
            }),
        ])),
        Err(ModelError::UnsupportedCommand(CommandType::RecordMarker))
    );
    assert_matches!(
        initial.complete_task(&completion(vec![
            command::Attributes::RecordMarkerCommandAttributes(RecordMarkerCommandAttributes {
                marker_name: PATCH_MARKER_NAME.to_owned(),
                ..Default::default()
            }),
        ])),
        Err(ModelError::InvalidCommand(CommandType::RecordMarker))
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

#[test]
fn remote_activity_results_branch_without_mutating_the_schedule() {
    let root = initial()
        .complete_task(&completion(vec![activity("first"), activity("second")]))
        .unwrap();
    let samples = BTreeMap::from([(
        "model-activity".to_owned(),
        ActivitySamples {
            results: vec![payloads("one"), payloads("two")],
            ..Default::default()
        },
    )]);
    let choices = root.activity_choices(&samples).unwrap();
    assert_eq!(choices.len(), 4);
    let first_id = 5;
    let second_id = 6;
    assert_eq!(
        choices[0],
        ModelEvent::CompleteActivity {
            scheduled_event_id: first_id,
            result: payloads("one"),
        }
    );
    assert_eq!(
        choices[3],
        ModelEvent::CompleteActivity {
            scheduled_event_id: second_id,
            result: payloads("two"),
        }
    );

    let branch = root
        .extend(&[choices[3].clone(), signal("after-activity")])
        .unwrap();
    assert_matches!(
        branch.history.events[6].attributes.as_ref(),
        Some(history_event::Attributes::ActivityTaskStartedEventAttributes(a))
            if a.scheduled_event_id == second_id && a.attempt == 1
    );
    assert_matches!(
        branch.history.events[7].attributes.as_ref(),
        Some(history_event::Attributes::ActivityTaskCompletedEventAttributes(a))
            if a.scheduled_event_id == second_id && a.started_event_id == 7
                && a.result == Some(payloads("two"))
    );
    let settled = branch.complete_task(&completion(vec![])).unwrap();
    assert_eq!(settled.activity_choices(&samples).unwrap(), choices[..2]);
    assert_eq!(root.activity_choices(&samples).unwrap(), choices);
    assert_matches!(
        settled.extend(&[choices[3].clone()]),
        Err(ModelError::ActivityNotPending(id)) if id == second_id
    );

    let restarted = settled
        .extend(&[signal("reuse")])
        .unwrap()
        .complete_task(&completion(vec![activity("second")]))
        .unwrap();
    let restarted_choices = restarted.activity_choices(&samples).unwrap();
    assert_eq!(restarted_choices.len(), 4);
    assert_ne!(restarted_choices[2], choices[2]);
}

#[test]
fn remote_activity_schedules_require_samples_and_unique_live_ids() {
    let root = initial()
        .complete_task(&completion(vec![activity("first")]))
        .unwrap();
    assert_matches!(
        root.activity_choices(&BTreeMap::new()),
        Err(ModelError::MissingActivityResults(name)) if name == "model-activity"
    );
    assert_matches!(
        root.activity_choices(&BTreeMap::from([(
            "model-activity".to_owned(),
            ActivitySamples::default(),
        )])),
        Err(ModelError::MissingActivityResults(_))
    );
    assert_matches!(
        root.extend(&[signal("duplicate")])
            .unwrap()
            .complete_task(&completion(vec![activity("first")])),
        Err(ModelError::DuplicateActivity(_))
    );
    assert_matches!(
        root.extend(&[ModelEvent::CompleteActivity {
            scheduled_event_id: 999,
            result: payloads("bad")
        }]),
        Err(ModelError::ActivityNotPending(999))
    );
    assert_matches!(
        initial().complete_task(&completion(vec![
            command::Attributes::ScheduleActivityTaskCommandAttributes(
                ScheduleActivityTaskCommandAttributes::default()
            )
        ])),
        Err(ModelError::InvalidCommand(
            CommandType::ScheduleActivityTask
        ))
    );
}

#[test]
fn failed_and_timed_out_activity_branches_are_terminal_and_isolated() {
    let mut schedule = activity("remote");
    let command::Attributes::ScheduleActivityTaskCommandAttributes(attributes) = &mut schedule
    else {
        unreachable!()
    };
    attributes.retry_policy = Some(RetryPolicy {
        maximum_attempts: 1,
        ..Default::default()
    });
    let root = initial()
        .complete_task(&completion(vec![schedule]))
        .unwrap();
    let application_failure = Failure {
        message: "declined".to_owned(),
        failure_info: Some(failure::FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                r#type: "Declined".to_owned(),
                details: Some(payloads("failure details")),
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    let timeout = Failure {
        failure_info: Some(failure::FailureInfo::TimeoutFailureInfo(
            TimeoutFailureInfo {
                timeout_type: TimeoutType::StartToClose as i32,
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    let samples = BTreeMap::from([(
        "model-activity".to_owned(),
        ActivitySamples {
            results: vec![payloads("accepted")],
            failures: vec![application_failure.clone(), timeout.clone()],
        },
    )]);
    let choices = root.activity_choices(&samples).unwrap();
    assert_eq!(choices.len(), 3);
    for choice in &choices[1..] {
        let branch = root.extend(std::slice::from_ref(choice)).unwrap();
        let started = &branch.history.events[5];
        let resolved = &branch.history.events[6];
        match resolved.attributes.as_ref().unwrap() {
            history_event::Attributes::ActivityTaskFailedEventAttributes(a) => {
                assert_eq!(a.failure.as_ref(), Some(&application_failure));
                assert_eq!(a.scheduled_event_id, 5);
                assert_eq!(a.started_event_id, started.event_id);
                assert_eq!(a.retry_state(), RetryState::MaximumAttemptsReached);
                assert_eq!(resolved.event_time, started.event_time);
            }
            history_event::Attributes::ActivityTaskTimedOutEventAttributes(a) => {
                assert_eq!(a.failure.as_ref(), Some(&timeout));
                assert_eq!(a.scheduled_event_id, 5);
                assert_eq!(a.started_event_id, started.event_id);
                assert_eq!(a.retry_state(), RetryState::MaximumAttemptsReached);
                assert_eq!(
                    resolved.event_time.unwrap().seconds - started.event_time.unwrap().seconds,
                    10
                );
            }
            other => panic!("unexpected activity outcome: {other:?}"),
        }
        let settled = branch.complete_task(&completion(vec![])).unwrap();
        assert!(
            settled
                .activity_choices(&BTreeMap::new())
                .unwrap()
                .is_empty()
        );
        assert_matches!(
            settled.extend(std::slice::from_ref(choice)),
            Err(ModelError::ActivityNotPending(5))
        );
        assert_eq!(root.pending_activities().len(), 1);
    }
}

#[test]
fn activity_failures_must_not_silently_skip_retries() {
    let root = initial()
        .complete_task(&completion(vec![activity("remote")]))
        .unwrap();
    let mut failure = Failure {
        failure_info: Some(failure::FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                r#type: "Declined".to_owned(),
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    assert_matches!(
        root.extend(&[ModelEvent::FailActivity {
            scheduled_event_id: 5,
            failure: failure.clone()
        }]),
        Err(ModelError::UnsupportedActivityFailure(_))
    );
    let mut schedule = activity("remote");
    let command::Attributes::ScheduleActivityTaskCommandAttributes(attributes) = &mut schedule
    else {
        unreachable!()
    };
    attributes.retry_policy = Some(RetryPolicy {
        non_retryable_error_types: vec!["Declined".to_owned()],
        ..Default::default()
    });
    let branch = initial()
        .complete_task(&completion(vec![schedule]))
        .unwrap()
        .extend(&[ModelEvent::FailActivity {
            scheduled_event_id: 5,
            failure: failure.clone(),
        }])
        .unwrap();
    assert_matches!(
        branch.history.events[6].attributes.as_ref(),
        Some(history_event::Attributes::ActivityTaskFailedEventAttributes(a))
            if a.retry_state() == RetryState::NonRetryableFailure
    );
    let Some(failure::FailureInfo::ApplicationFailureInfo(info)) = &mut failure.failure_info else {
        unreachable!()
    };
    info.non_retryable = true;
    let branch = root
        .extend(&[ModelEvent::FailActivity {
            scheduled_event_id: 5,
            failure,
        }])
        .unwrap();
    assert_matches!(
        branch.history.events[6].attributes.as_ref(),
        Some(history_event::Attributes::ActivityTaskFailedEventAttributes(a))
            if a.retry_state() == RetryState::NonRetryableFailure
    );
    assert_matches!(
        root.extend(&[ModelEvent::FailActivity {
            scheduled_event_id: 5,
            failure: Failure {
                failure_info: Some(failure::FailureInfo::TimeoutFailureInfo(
                    TimeoutFailureInfo {
                        timeout_type: TimeoutType::Heartbeat as i32,
                        ..Default::default()
                    }
                )),
                ..Default::default()
            },
        }]),
        Err(ModelError::UnsupportedActivityFailure(_))
    );
}

#[tokio::test]
async fn generated_remote_activity_result_replays_through_core() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let expected = payloads("sampled result");
        let model = initial()
            .complete_task(&completion(vec![activity("remote")]))
            .unwrap()
            .extend(&[ModelEvent::CompleteActivity {
                scheduled_event_id: 5,
                result: expected.clone(),
            }])
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
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                activation.run_id,
                workflow_commands::ScheduleActivity {
                    seq: 1,
                    activity_id: "remote".to_owned(),
                    activity_type: "model-activity".to_owned(),
                    task_queue: "model-queue".to_owned(),
                    start_to_close_timeout: Some(Duration::from_secs(10).try_into().unwrap()),
                    ..Default::default()
                }
                .into(),
            ))
            .await
            .unwrap();
        let activation = worker.poll_workflow_activation().await.unwrap();
        assert_matches!(
            activation.jobs[0].variant.as_ref(),
            Some(workflow_activation_job::Variant::ResolveActivity(a))
                if matches!(
                    a.result.as_ref().unwrap().status.as_ref().unwrap(),
                    activity_resolution::Status::Completed(c)
                        if c.result.as_ref() == expected.payloads.first()
                )
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
            activation.jobs[0].variant,
            Some(workflow_activation_job::Variant::RemoveFromCache(_))
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
    .expect("generated activity history replay did not finish");
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
