//! Event Groups user-facing SDK tests.
//!
//! Mocked tests check that `event_groups` on command options reach Core commands. History tests
//! cover label IDs, scopes, aggregation, implicit handlers, and command-type coverage for every
//! family the Rust SDK exposes.

use std::{collections::HashSet, time::Duration};

use crate::{
    common::{
        CoreWfStarter, SEARCH_ATTR_TXT, activity_functions::StdActivities,
        build_fake_sdk_with_options, mock_sdk_cfg_with_options,
    },
    integ_tests::mk_nexus_endpoint,
};
use anyhow::anyhow;
use sha1::{Digest, Sha1};
use temporalio_client::{
    UntypedWorkflow, WorkflowExecuteUpdateOptions, WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::{
    RetryPolicy,
    data_converters::RawValue,
    protos::{
        coresdk::{
            AsJsonPayloadExt,
            common::extract_local_activity_marker_data,
            nexus::{NexusTaskCompletion, nexus_task_completion},
        },
        temporal::api::{
            enums::v1::{CommandType, EventType},
            history::v1::{History, HistoryEvent, history_event},
            nexus,
            nexus::v1::{
                CancelOperationResponse, StartOperationResponse, request, start_operation_response,
            },
            sdk::v1::{
                EventGroupMarker,
                event_group_marker::{Label, Variant},
            },
        },
    },
    search_attributes::SearchAttributeKey,
    worker::WorkerTaskTypes,
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityCancellationType, ActivityOptions, CancelExternalWorkflowOptions,
    ChildWorkflowCancellationType, ChildWorkflowOptions, ContinueAsNewOptions, EventGroup,
    LocalActivityOptions, MemoValue, NexusOperationCancellationType, NexusOperationOptions,
    SignalWorkflowOptions, SyncWorkflowContext, TimerOptions, WaitConditionOptions,
    WorkflowCancellationToken, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
    workflows::join,
};
use temporalio_sdk_core::{
    PollError,
    replay::{DEFAULT_WORKFLOW_TYPE, canned_histories},
    test_help::MockPollCfg,
};

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn pass_event_group_markers_on_schedule_activity() {
    let t = canned_histories::single_activity("1");
    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    let wf_id = mock_cfg.hists[0].wf_id.clone();
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let expected_groups = vec![EventGroup::with_id(
        "activity-group-label",
        "activity-group",
    )];
    let expected_markers = vec![label_marker("activity-group", "activity-group-label")];

    let expected_for_assert = expected_markers.clone();
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type(),
                    CommandType::ScheduleActivityTask
                );
                assert_eq!(wft.commands[0].event_group_markers, expected_for_assert);
            })
            .then(|wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
                assert!(wft.commands[0].event_group_markers.is_empty());
            });
    });

    #[workflow]
    struct ActivityWithGroupWorkflow {
        event_groups: Vec<EventGroup>,
    }

    #[workflow_methods(factory_only)]
    impl ActivityWithGroupWorkflow {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let event_groups = ctx.state(|wf| wf.event_groups.clone());
            ctx.execute_activity(
                StdActivities::default,
                (),
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                    .event_groups(event_groups)
                    .build(),
            )
            .await?;
            Ok(())
        }
    }

    let mut worker = mock_sdk_cfg_with_options(
        mock_cfg,
        |_| {},
        |options| {
            options
                .register_workflow_with_factory(move || ActivityWithGroupWorkflow {
                    event_groups: expected_groups.clone(),
                })
                .unwrap();
        },
    );
    let task_queue = worker.inner_mut().task_queue().to_owned();
    worker
        .submit_wf(
            wf_type.to_owned(),
            vec![],
            WorkflowStartOptions::new(task_queue, wf_id.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn pass_event_group_markers_on_start_child_workflow() {
    let wf_id = "1";
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let t = canned_histories::single_child_workflow(wf_id);
    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    let expected_groups = vec![EventGroup::with_id("child-group-label", "child-group")];
    let expected_markers = vec![label_marker("child-group", "child-group-label")];

    let expected_for_assert = expected_markers.clone();
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type(),
                    CommandType::StartChildWorkflowExecution
                );
                assert_eq!(wft.commands[0].event_group_markers, expected_for_assert);
            })
            .then(|wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
                assert!(wft.commands[0].event_group_markers.is_empty());
            });
    });

    #[workflow]
    struct ChildWithGroupWorkflow {
        child_wf_id: String,
        event_groups: Vec<EventGroup>,
    }

    #[workflow_methods(factory_only)]
    impl ChildWithGroupWorkflow {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let (child_wf_id, event_groups) =
                ctx.state(|wf| (wf.child_wf_id.clone(), wf.event_groups.clone()));
            ctx.start_child_workflow(
                UntypedWorkflow::new("child"),
                RawValue::new(vec![]),
                ChildWorkflowOptions::builder()
                    .workflow_id(child_wf_id)
                    .event_groups(event_groups)
                    .build(),
            )
            .await?;
            Ok(())
        }
    }

    let child_wf_id = wf_id.to_string();
    let event_groups_for_wf = expected_groups.clone();
    let mut worker = mock_sdk_cfg_with_options(
        mock_cfg,
        |_| {},
        |options| {
            options
                .register_workflow_with_factory(move || ChildWithGroupWorkflow {
                    child_wf_id: child_wf_id.clone(),
                    event_groups: event_groups_for_wf.clone(),
                })
                .unwrap();
        },
    );
    let task_queue = worker.inner_mut().task_queue().to_owned();
    worker
        .submit_wf(
            wf_type.to_owned(),
            vec![],
            WorkflowStartOptions::new(task_queue, wf_id.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn pass_event_group_markers_on_start_timer() {
    let t = canned_histories::single_timer("1");
    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    let wf_id = mock_cfg.hists[0].wf_id.clone();
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let expected_groups = vec![EventGroup::with_id("timer-group-label", "timer-group")];
    let expected_markers = vec![label_marker("timer-group", "timer-group-label")];

    let expected_for_assert = expected_markers.clone();
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(wft.commands[0].command_type(), CommandType::StartTimer);
                assert_eq!(wft.commands[0].event_group_markers, expected_for_assert);
            })
            .then(|wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
                assert!(wft.commands[0].event_group_markers.is_empty());
            });
    });

    #[workflow]
    struct TimerWithGroupWorkflow {
        event_groups: Vec<EventGroup>,
    }

    #[workflow_methods(factory_only)]
    impl TimerWithGroupWorkflow {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let event_groups = ctx.state(|wf| wf.event_groups.clone());
            ctx.timer(
                TimerOptions::builder(Duration::from_secs(1))
                    .event_groups(event_groups)
                    .build(),
            )
            .await;
            Ok(())
        }
    }

    let event_groups_for_wf = expected_groups.clone();
    let mut worker = mock_sdk_cfg_with_options(
        mock_cfg,
        |_| {},
        |options| {
            options
                .register_workflow_with_factory(move || TimerWithGroupWorkflow {
                    event_groups: event_groups_for_wf.clone(),
                })
                .unwrap();
        },
    );
    let task_queue = worker.inner_mut().task_queue().to_owned();
    worker
        .submit_wf(
            wf_type.to_owned(),
            vec![],
            WorkflowStartOptions::new(task_queue, wf_id.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

/// Local activities pose some particular challenges: the corresponding `RecordMarker` command
/// only gets created at a later point, after the local activity completes execution.
#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn pass_event_group_markers_on_schedule_local_activity() {
    let t = canned_histories::single_local_activity("1");
    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    let expected_groups = vec![EventGroup::with_id(
        "local-activity-label",
        "local-activity-group",
    )];
    let expected_markers = vec![label_marker("local-activity-group", "local-activity-label")];

    let expected_for_assert = expected_markers.clone();
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(move |wft| {
            assert_eq!(wft.commands.len(), 2);
            assert_eq!(wft.commands[0].command_type(), CommandType::RecordMarker);
            assert_eq!(wft.commands[0].event_group_markers, expected_for_assert);
            assert_eq!(
                wft.commands[1].command_type(),
                CommandType::CompleteWorkflowExecution
            );
            assert!(wft.commands[1].event_group_markers.is_empty());
        });
    });

    #[workflow]
    struct LocalActivityWithGroupWorkflow {
        event_groups: Vec<EventGroup>,
    }

    #[workflow_methods(factory_only)]
    impl LocalActivityWithGroupWorkflow {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let event_groups = ctx.state(|wf| wf.event_groups.clone());
            ctx.execute_local_activity(
                StdActivities::default,
                (),
                LocalActivityOptions::builder()
                    .event_groups(event_groups)
                    .build(),
            )
            .await?;
            Ok(())
        }
    }

    let mut worker = build_fake_sdk_with_options(mock_cfg, |options| {
        options
            .register_workflow_with_factory(move || LocalActivityWithGroupWorkflow {
                event_groups: expected_groups.clone(),
            })
            .unwrap()
            .register_activities(StdActivities);
    });
    worker.run().await.unwrap();
}

const PERSIST_TEST_MARKER_ID: &str = "persist-test";
const PERSIST_TEST_MARKER_LABEL: &str = "persist-test-label";
const PERSIST_TEST_LA_MARKER_ID: &str = "persist-test-la";
const PERSIST_TEST_LA_MARKER_LABEL: &str = "persist-test-la-label";

#[workflow]
#[derive(Default)]
pub(crate) struct ActivityEventGroupPersistsWf;

#[workflow_methods]
impl ActivityEventGroupPersistsWf {
    #[run(name = "event_group_markers_persist_to_history_events")]
    pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.execute_activity(
            StdActivities::default,
            (),
            ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                .event_groups(vec![EventGroup::with_id(
                    PERSIST_TEST_MARKER_LABEL,
                    PERSIST_TEST_MARKER_ID,
                )])
                .build(),
        )
        .await?;
        ctx.execute_local_activity(
            StdActivities::default,
            (),
            LocalActivityOptions::builder()
                .start_to_close_timeout(Duration::from_secs(5))
                .event_groups(vec![EventGroup::with_id(
                    PERSIST_TEST_LA_MARKER_LABEL,
                    PERSIST_TEST_LA_MARKER_ID,
                )])
                .build(),
        )
        .await?;
        Ok(())
    }
}

#[tokio::test]
async fn event_group_markers_persist_to_history_events() {
    let wf_name = "event_group_markers_persist_to_history_events";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .sdk_config
        .register_activities(StdActivities)
        .register_workflow::<ActivityEventGroupPersistsWf>()
        .unwrap();
    let mut worker = starter.worker().await;

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();

    let history = starter.get_history().await;
    let scheduled_events: Vec<_> = history
        .events
        .iter()
        .filter(|e| e.event_type() == EventType::ActivityTaskScheduled)
        .collect();
    assert_eq!(scheduled_events.len(), 1);
    assert_eq!(
        scheduled_events[0].event_group_markers,
        vec![label_marker(
            PERSIST_TEST_MARKER_ID,
            PERSIST_TEST_MARKER_LABEL
        )]
    );

    let marker_events: Vec<_> = history
        .events
        .iter()
        .filter(|e| e.event_type() == EventType::MarkerRecorded)
        .collect();
    assert_eq!(marker_events.len(), 1);
    assert_eq!(
        marker_events[0].event_group_markers,
        vec![label_marker(
            PERSIST_TEST_LA_MARKER_ID,
            PERSIST_TEST_LA_MARKER_LABEL
        )]
    );
}

#[workflow]
#[derive(Default)]
struct LabelsAndScopesWf;

#[workflow_methods]
impl LabelsAndScopesWf {
    #[run(name = "event_groups_labels_and_scopes")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let payment = ctx.create_event_group("payment-processing");
        let same_a = ctx.create_event_group("bbb");
        let same_b = ctx.create_event_group("bbb");
        let customer = EventGroup::with_id("customer-james-watkins", "customer-123456");

        ctx.execute_activity(
            StdActivities::echo,
            "activity-a".to_string(),
            ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                .event_groups(vec![payment.clone()])
                .build(),
        )
        .await?;
        ctx.execute_activity(
            StdActivities::echo,
            "activity-b1".to_string(),
            ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                .event_groups(vec![same_a])
                .build(),
        )
        .await?;
        ctx.execute_activity(
            StdActivities::echo,
            "activity-b2".to_string(),
            ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                .event_groups(vec![same_b])
                .build(),
        )
        .await?;
        ctx.execute_activity(
            StdActivities::echo,
            "explicit".to_string(),
            ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                .event_groups(vec![payment.clone(), customer.clone()])
                .build(),
        )
        .await?;

        let scoped = ctx.with_event_group(payment.clone());
        scoped
            .execute_activity(
                StdActivities::echo,
                "scoped".to_string(),
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5)).build(),
            )
            .await?;
        scoped
            .execute_activity(
                StdActivities::echo,
                "scoped-and-explicit".to_string(),
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                    .event_groups(vec![payment.clone()])
                    .build(),
            )
            .await?;
        let nested = scoped.with_event_group(customer);
        nested.timer(Duration::from_millis(1)).await;
        ctx.execute_activity(
            StdActivities::echo,
            "unscoped".to_string(),
            ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5)).build(),
        )
        .await?;
        Ok(())
    }
}

#[tokio::test]
async fn event_groups_labels_scopes_and_aggregation() {
    let wf_name = "event_groups_labels_and_scopes";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .sdk_config
        .register_activities(StdActivities)
        .register_workflow::<LabelsAndScopesWf>()
        .unwrap();
    let mut worker = starter.worker().await;
    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();

    let history = starter.get_history().await;
    let original_run_id = original_execution_run_id(&history);
    let payment_id = derived_group_id(original_run_id, "payment-processing");
    let bbb_id = derived_group_id(original_run_id, "bbb");
    let scheduled = activities_by_input(&history);
    assert_eq!(
        label_ids(scheduled["activity-a"]),
        set([payment_id.clone()])
    );
    assert_eq!(
        label_ids(scheduled["activity-b1"]),
        label_ids(scheduled["activity-b2"])
    );
    assert_eq!(label_ids(scheduled["activity-b1"]), set([bbb_id]));
    assert_eq!(
        label_ids(scheduled["explicit"]),
        set([payment_id.clone(), "customer-123456".to_string()])
    );
    assert_eq!(label_ids(scheduled["scoped"]), set([payment_id.clone()]));
    assert_eq!(
        label_ids(scheduled["scoped-and-explicit"]),
        set([payment_id.clone()])
    );
    assert!(label_ids(scheduled["unscoped"]).is_empty());

    let timer = history
        .events
        .iter()
        .find(|e| e.event_type() == EventType::TimerStarted)
        .expect("nested-scope timer");
    assert_eq!(
        label_ids(timer),
        set([payment_id, "customer-123456".to_string()])
    );

    let a_label = scheduled["activity-a"].event_group_markers[0]
        .variant
        .as_ref()
        .and_then(|variant| match variant {
            Variant::Label(label) => label.label.as_ref(),
            _ => None,
        })
        .expect("label payload");
    assert_eq!(
        a_label.metadata.get("encoding").map(Vec::as_slice),
        Some(b"json/plain".as_slice())
    );
    assert_eq!(a_label.data, b"\"payment-processing\"");
}

#[workflow]
#[derive(Default)]
struct CommandsWf;

#[workflow]
#[derive(Default)]
struct CommandsChildWf;

#[workflow_methods]
impl CommandsChildWf {
    #[run]
    async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        Ok(())
    }
}

#[workflow_methods]
impl CommandsWf {
    #[run(name = "event_groups_commands")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let group = EventGroup::with_id("command-label", "command-id");
        ctx.timer(
            TimerOptions::builder(Duration::from_millis(1))
                .event_groups(vec![group.clone()])
                .build(),
        )
        .await;
        ctx.execute_activity(
            StdActivities::default,
            (),
            ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                .event_groups(vec![group.clone()])
                .build(),
        )
        .await?;
        ctx.execute_local_activity(
            StdActivities::default,
            (),
            LocalActivityOptions::builder()
                .start_to_close_timeout(Duration::from_secs(5))
                .event_groups(vec![group.clone()])
                .build(),
        )
        .await?;
        let started = ctx
            .start_child_workflow(
                CommandsChildWf::run,
                (),
                ChildWorkflowOptions::builder()
                    .event_groups(vec![group])
                    .build(),
            )
            .await
            .expect("child starts");
        started.result().await?;
        Ok(())
    }
}

#[tokio::test]
async fn event_groups_attach_to_timer_activity_la_and_child() {
    let wf_name = "event_groups_commands";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .sdk_config
        .register_activities(StdActivities)
        .register_workflow::<CommandsWf>()
        .unwrap();
    starter
        .sdk_config
        .register_workflow::<CommandsChildWf>()
        .unwrap();
    let mut worker = starter.worker().await;
    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();

    let history = starter.get_history().await;
    let expected = set(["command-id".to_string()]);
    let timer = history
        .events
        .iter()
        .find(|e| e.event_type() == EventType::TimerStarted)
        .unwrap();
    assert_eq!(label_ids(timer), expected);
    let activity = history
        .events
        .iter()
        .find(|e| e.event_type() == EventType::ActivityTaskScheduled)
        .unwrap();
    assert_eq!(label_ids(activity), expected);
    let local = history
        .events
        .iter()
        .find(|e| e.event_type() == EventType::MarkerRecorded)
        .unwrap();
    assert_eq!(label_ids(local), expected);
    let child = history
        .events
        .iter()
        .find(|e| e.event_type() == EventType::StartChildWorkflowExecutionInitiated)
        .unwrap();
    assert_eq!(label_ids(child), expected);
}

#[workflow]
#[derive(Default)]
struct ImplicitHandlersWf {
    done: bool,
}

#[workflow_methods]
impl ImplicitHandlersWf {
    #[run(name = "event_groups_implicit_handlers")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.wait_condition(|wf| wf.done).await?;
        Ok(())
    }

    #[signal]
    async fn ping(ctx: &mut WorkflowContext<Self>) {
        ctx.timer(Duration::from_millis(1)).await;
    }

    #[update]
    async fn poke(
        ctx: &mut WorkflowContext<Self>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        ctx.timer(Duration::from_millis(1)).await;
        Ok(())
    }

    #[signal]
    fn done(&mut self, _ctx: &mut temporalio_sdk::SyncWorkflowContext<Self>) {
        self.done = true;
    }
}

#[tokio::test]
async fn event_groups_implicit_signal_and_update_handlers() {
    let wf_name = "event_groups_implicit_handlers";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .sdk_config
        .register_workflow::<ImplicitHandlersWf>()
        .unwrap();
    let mut worker = starter.worker().await;
    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            ImplicitHandlersWf::run,
            (),
            WorkflowStartOptions::new(task_queue, starter.get_wf_id().to_owned()).build(),
        )
        .await
        .unwrap();
    let drive = async {
        handle
            .signal(
                ImplicitHandlersWf::ping,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
        handle
            .execute_update(
                ImplicitHandlersWf::poke,
                (),
                WorkflowExecuteUpdateOptions::default(),
            )
            .await
            .unwrap();
        handle
            .signal(
                ImplicitHandlersWf::done,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
    };
    let run = async {
        worker.run_until_done().await.unwrap();
    };
    tokio::join!(drive, run);

    let history = starter.get_history().await;
    let signal_event = history
        .events
        .iter()
        .find(|e| {
            e.event_type() == EventType::WorkflowExecutionSignaled && signal_name(e) == Some("ping")
        })
        .expect("ping signal event");
    let update_id = history
        .events
        .iter()
        .find_map(|e| match e.attributes.as_ref()? {
            history_event::Attributes::WorkflowExecutionUpdateAcceptedEventAttributes(attrs) => {
                Some(
                    attrs
                        .accepted_request
                        .as_ref()?
                        .meta
                        .as_ref()?
                        .update_id
                        .clone(),
                )
            }
            _ => None,
        })
        .expect("accepted update id");

    let timers: Vec<_> = history
        .events
        .iter()
        .filter(|e| e.event_type() == EventType::TimerStarted)
        .collect();
    assert_eq!(timers.len(), 2);
    let signal_timer = timers
        .iter()
        .find(|e| inbound_event_id(e) == Some(signal_event.event_id))
        .expect("timer inherits inbound signal group");
    assert!(label_ids(signal_timer).is_empty());
    let update_timer = timers
        .iter()
        .find(|e| inbound_update_id(e).as_deref() == Some(update_id.as_str()))
        .expect("timer inherits inbound update group");
    assert!(label_ids(update_timer).is_empty());
}

#[workflow]
#[derive(Default)]
struct CancelAndMetadataWf;

#[workflow_methods]
impl CancelAndMetadataWf {
    #[run(name = "event_groups_cancel_and_metadata")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let scope = EventGroup::with_id("scope-label", "scope-id");
        let direct = EventGroup::with_id("direct-label", "direct-id");
        let scoped = ctx.with_event_group(scope);

        let timer_token = WorkflowCancellationToken::new();
        let mut long_timer_opts = TimerOptions::builder(Duration::from_secs(60))
            .event_groups(vec![direct.clone()])
            .build();
        long_timer_opts.cancellation_token = Some(timer_token.clone());
        let long_timer = scoped.timer(long_timer_opts);
        scoped.timer(Duration::from_millis(1)).await;
        timer_token.cancel();
        let _ = long_timer.await;

        let activity_token = WorkflowCancellationToken::new();
        let mut activity_opts =
            ActivityOptions::with_start_to_close_timeout(Duration::from_secs(10))
                .cancellation_type(ActivityCancellationType::TryCancel)
                .event_groups(vec![direct.clone()])
                .build();
        activity_opts.cancellation_token = Some(activity_token.clone());
        let activity =
            scoped.execute_activity(StdActivities::delay, Duration::from_secs(5), activity_opts);
        scoped.timer(Duration::from_millis(1)).await;
        activity_token.cancel();
        let _ = activity.await;

        let _ = scoped
            .external_workflow("missing-eg-signal-target", None)
            .signal(
                Self::ping,
                (),
                SignalWorkflowOptions::builder()
                    .event_groups(vec![direct.clone()])
                    .build(),
            )
            .await;
        let _ = scoped
            .external_workflow("missing-eg-cancel-target", None)
            .cancel_with_options(
                CancelExternalWorkflowOptions::builder()
                    .event_groups(vec![direct])
                    .build(),
            )
            .await;

        scoped.upsert_memo([("eg-memo", Some(MemoValue::new("eg-value".to_string())))])?;
        scoped.upsert_search_attributes([
            SearchAttributeKey::text(SEARCH_ATTR_TXT).value_set("eg-sa".to_string())
        ]);
        let _ = scoped.patched("eg-patch-1");
        let _ = scoped.deprecate_patch("eg-patch-2");
        Ok(())
    }

    #[signal]
    fn ping(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {}
}

#[tokio::test]
async fn event_groups_cancel_external_upsert_and_patch_commands() {
    let wf_name = "event_groups_cancel_and_metadata";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .sdk_config
        .register_activities(StdActivities)
        .register_workflow::<CancelAndMetadataWf>()
        .unwrap();
    let mut worker = starter.worker().await;
    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();

    let history = starter.get_history().await;
    let scope_and_direct = set(["scope-id", "direct-id"]);
    let scope_only = set(["scope-id"]);

    let timers: Vec<_> = history
        .events
        .iter()
        .filter(|e| e.event_type() == EventType::TimerStarted)
        .collect();
    let timeout_timer = timers
        .iter()
        .find(|e| timer_duration_millis(e) == Some(1))
        .expect("1ms timeout timer");
    let cancelled_timer = timers
        .iter()
        .find(|e| timer_duration_millis(e) == Some(60_000))
        .expect("60s cancelled timer");
    assert_eq!(label_ids(timeout_timer), scope_only);
    assert_eq!(label_ids(cancelled_timer), scope_and_direct);

    let timer_canceled = history
        .events
        .iter()
        .find(|e| e.event_type() == EventType::TimerCanceled)
        .expect("timer canceled");
    assert_eq!(label_ids(timer_canceled), scope_and_direct);

    let scheduled = history
        .events
        .iter()
        .find(|e| e.event_type() == EventType::ActivityTaskScheduled)
        .expect("activity scheduled");
    assert_eq!(label_ids(scheduled), scope_and_direct);
    let activity_cancel = history
        .events
        .iter()
        .find(|e| e.event_type() == EventType::ActivityTaskCancelRequested)
        .expect("activity cancel requested");
    assert_eq!(label_ids(activity_cancel), scope_and_direct);

    let signal_ext = history
        .events
        .iter()
        .find(|e| e.event_type() == EventType::SignalExternalWorkflowExecutionInitiated)
        .expect("signal external initiated");
    assert_eq!(label_ids(signal_ext), scope_and_direct);
    let cancel_ext = history
        .events
        .iter()
        .find(|e| e.event_type() == EventType::RequestCancelExternalWorkflowExecutionInitiated)
        .expect("cancel external initiated");
    assert_eq!(label_ids(cancel_ext), scope_and_direct);

    let memo = history
        .events
        .iter()
        .find(|e| e.event_type() == EventType::WorkflowPropertiesModified)
        .expect("memo upsert");
    assert_eq!(label_ids(memo), scope_only);

    let sa_upserts: Vec<_> = history
        .events
        .iter()
        .filter(|e| e.event_type() == EventType::UpsertWorkflowSearchAttributes)
        .collect();
    let user_sa = sa_upserts
        .iter()
        .find(|e| !search_attribute_keys(e).contains("TemporalChangeVersion"))
        .expect("user search-attribute upsert");
    assert_eq!(label_ids(user_sa), scope_only);
    for patch_sa in sa_upserts
        .iter()
        .filter(|e| search_attribute_keys(e).contains("TemporalChangeVersion"))
    {
        assert_eq!(label_ids(patch_sa), scope_only);
    }

    let markers: Vec<_> = history
        .events
        .iter()
        .filter(|e| e.event_type() == EventType::MarkerRecorded)
        .collect();
    assert!(markers.len() >= 2, "patched and deprecate_patch markers");
    for marker in markers {
        assert_eq!(label_ids(marker), scope_only);
    }
}

#[workflow]
#[derive(Default)]
struct WaitTimeoutWf;

#[workflow_methods]
impl WaitTimeoutWf {
    #[run(name = "event_groups_wait_timeout")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let scope = EventGroup::with_id("scope-label", "scope-id");
        let direct = EventGroup::with_id("direct-label", "direct-id");
        let scoped = ctx.with_event_group(scope);
        let _ = scoped
            .wait_condition_with_options(
                |_| false,
                WaitConditionOptions::builder()
                    .timeout(Duration::from_millis(1))
                    .event_groups(vec![direct])
                    .build(),
            )
            .await?;
        Ok(())
    }
}

#[tokio::test]
async fn event_groups_wait_condition_timeout_timer() {
    let wf_name = "event_groups_wait_timeout";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .sdk_config
        .register_workflow::<WaitTimeoutWf>()
        .unwrap();
    let mut worker = starter.worker().await;
    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();

    let history = starter.get_history().await;
    let timer = history
        .events
        .iter()
        .find(|e| e.event_type() == EventType::TimerStarted)
        .expect("wait-condition timeout timer");
    assert_eq!(label_ids(timer), set(["scope-id", "direct-id"]));
}

struct EventGroupActivities;

#[activities]
impl EventGroupActivities {
    #[activity]
    async fn fail_first(ctx: ActivityContext) -> Result<(), ActivityError> {
        if ctx.info().attempt == 1 {
            return Err(anyhow!("fail first attempt").into());
        }
        Ok(())
    }
}

#[workflow]
#[derive(Default)]
struct LocalActivityCancelBackoffWf;

#[workflow_methods]
impl LocalActivityCancelBackoffWf {
    #[run(name = "event_groups_la_cancel_backoff")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let scope = EventGroup::with_id("scope-label", "scope-id");
        let direct = EventGroup::with_id("direct-label", "direct-id");
        let scoped = ctx.with_event_group(scope);

        scoped
            .execute_local_activity(
                StdActivities::no_op,
                (),
                LocalActivityOptions::builder()
                    .start_to_close_timeout(Duration::from_secs(10))
                    .activity_id("local-activity".to_string())
                    .event_groups(vec![direct.clone()])
                    .build(),
            )
            .await?;

        let sleeping_token = WorkflowCancellationToken::new();
        let sleeping_ctx = scoped.clone();
        let trigger_ctx = scoped.clone();
        let mut sleeping_opts = LocalActivityOptions::builder()
            .start_to_close_timeout(Duration::from_secs(10))
            .cancel_type(ActivityCancellationType::TryCancel)
            .activity_id("cancelled-local-activity-sleep-5s".to_string())
            .event_groups(vec![
                direct.clone(),
                EventGroup::with_id("cancelled-la", "cancelled-la"),
            ])
            .build();
        sleeping_opts.cancellation_token = Some(sleeping_token.clone());
        let sleeping = sleeping_ctx.execute_local_activity(
            StdActivities::delay,
            Duration::from_secs(5),
            sleeping_opts,
        );
        let trigger = async {
            trigger_ctx
                .execute_local_activity(
                    StdActivities::no_op,
                    (),
                    LocalActivityOptions::builder()
                        .start_to_close_timeout(Duration::from_secs(10))
                        .activity_id("cancel-trigger".to_string())
                        .event_groups(vec![
                            direct.clone(),
                            EventGroup::with_id("cancel-trigger", "cancel-trigger"),
                        ])
                        .build(),
                )
                .await?;
            sleeping_token.cancel();
            Ok::<(), temporalio_sdk::ActivityExecutionError>(())
        };
        let (trigger_res, sleep_res) = join!(trigger, sleeping);
        trigger_res?;
        let _ = sleep_res;

        scoped
            .execute_local_activity(
                EventGroupActivities::fail_first,
                (),
                LocalActivityOptions::builder()
                    .start_to_close_timeout(Duration::from_secs(10))
                    .timer_backoff_threshold(Duration::from_millis(1))
                    .retry_policy(
                        RetryPolicy::builder()
                            .initial_interval(Duration::from_secs(1))
                            .backoff_coefficient(1.0)
                            .maximum_attempts(2)
                            .build(),
                    )
                    .activity_id("backoff-local-activity-fail-first-attempt".to_string())
                    .event_groups(vec![direct])
                    .build(),
            )
            .await?;
        Ok(())
    }
}

#[tokio::test]
async fn event_groups_local_activity_cancel_and_backoff() {
    let wf_name = "event_groups_la_cancel_backoff";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.workflow_options.task_timeout = Some(Duration::from_secs(5));
    starter
        .sdk_config
        .register_activities(StdActivities)
        .register_activities(EventGroupActivities)
        .register_workflow::<LocalActivityCancelBackoffWf>()
        .unwrap();
    let mut worker = starter.worker().await;
    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();

    let history = starter.get_history().await;
    let both = set(["scope-id", "direct-id"]);
    let cancel_trigger = set(["scope-id", "direct-id", "cancel-trigger"]);
    let cancelled_la = set(["scope-id", "direct-id", "cancelled-la"]);

    assert_eq!(
        label_ids(local_activity_by_id(&history, "local-activity")),
        both
    );
    assert_eq!(
        label_ids(local_activity_by_id(&history, "cancel-trigger")),
        cancel_trigger
    );
    assert_eq!(
        label_ids(local_activity_by_id(
            &history,
            "cancelled-local-activity-sleep-5s"
        )),
        cancelled_la
    );

    let backoff = local_activities_by_id(&history, "backoff-local-activity-fail-first-attempt");
    assert_eq!(backoff.len(), 2);
    for marker in &backoff {
        assert_eq!(label_ids(marker), both);
    }
    let timers: Vec<_> = history
        .events
        .iter()
        .filter(|e| e.event_type() == EventType::TimerStarted)
        .collect();
    assert_eq!(timers.len(), 1);
    assert_eq!(label_ids(timers[0]), both);
}

#[workflow]
#[derive(Default)]
struct ChildCancelWf;

#[workflow]
#[derive(Default)]
struct ChildCancelNoopChildWf;

#[workflow]
#[derive(Default)]
struct ChildCancelSleepChildWf;

#[workflow_methods]
impl ChildCancelNoopChildWf {
    #[run]
    async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        Ok(())
    }
}

#[workflow_methods]
impl ChildCancelSleepChildWf {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.timer(Duration::from_secs(60)).await;
        Ok(())
    }
}

#[workflow_methods]
impl ChildCancelWf {
    #[run(name = "event_groups_child_cancel")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let scope = EventGroup::with_id("scope-label", "scope-id");
        let direct = EventGroup::with_id("direct-label", "direct-id");
        let scoped = ctx.with_event_group(scope);

        let started = scoped
            .start_child_workflow(
                ChildCancelNoopChildWf::run,
                (),
                ChildWorkflowOptions::builder()
                    .event_groups(vec![direct.clone()])
                    .build(),
            )
            .await
            .expect("noop child starts");
        started.result().await?;

        let started = scoped
            .start_child_workflow(
                ChildCancelSleepChildWf::run,
                (),
                ChildWorkflowOptions::builder()
                    .cancel_type(ChildWorkflowCancellationType::WaitCancellationRequested)
                    .event_groups(vec![direct])
                    .build(),
            )
            .await
            .expect("sleeping child starts");
        scoped.timer(Duration::from_millis(1)).await;
        started.cancel("event-groups-child-cancel".to_string());
        let _ = started.result().await;
        Ok(())
    }
}

#[tokio::test]
async fn event_groups_child_workflow_cancel() {
    let wf_name = "event_groups_child_cancel";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .sdk_config
        .register_workflow::<ChildCancelWf>()
        .unwrap();
    starter
        .sdk_config
        .register_workflow::<ChildCancelNoopChildWf>()
        .unwrap();
    starter
        .sdk_config
        .register_workflow::<ChildCancelSleepChildWf>()
        .unwrap();
    let mut worker = starter.worker().await;
    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();

    let history = starter.get_history().await;
    let both = set(["scope-id", "direct-id"]);
    let initiated: Vec<_> = history
        .events
        .iter()
        .filter(|e| e.event_type() == EventType::StartChildWorkflowExecutionInitiated)
        .collect();
    assert_eq!(initiated.len(), 2);
    for event in initiated {
        assert_eq!(label_ids(event), both);
    }
    let cancel = history
        .events
        .iter()
        .find(|e| e.event_type() == EventType::RequestCancelExternalWorkflowExecutionInitiated)
        .expect("child cancel requested");
    assert_eq!(label_ids(cancel), both);
}

#[workflow]
#[derive(Default)]
struct NexusCommandsWf;

#[workflow_methods]
impl NexusCommandsWf {
    #[run(name = "event_groups_nexus_commands")]
    async fn run(ctx: &mut WorkflowContext<Self>, endpoint: String) -> WorkflowResult<()> {
        let scope = EventGroup::with_id("scope-label", "scope-id");
        let direct = EventGroup::with_id("direct-label", "direct-id");
        let scoped = ctx.with_event_group(scope);

        let started = scoped
            .start_nexus_operation(
                NexusOperationOptions::builder()
                    .endpoint(endpoint.clone())
                    .service("svc")
                    .operation("op")
                    .schedule_to_close_timeout(Duration::from_secs(5))
                    .event_groups(vec![direct.clone()])
                    .build(),
            )
            .await
            .expect("nexus start succeeds");
        let _ = started.result().await;

        let started = scoped
            .start_nexus_operation(
                NexusOperationOptions::builder()
                    .endpoint(endpoint)
                    .service("svc")
                    .operation("sleep")
                    .schedule_to_close_timeout(Duration::from_secs(10))
                    .cancellation_type(NexusOperationCancellationType::TryCancel)
                    .event_groups(vec![direct])
                    .build(),
            )
            .await
            .expect("sleeping nexus start succeeds");
        scoped.timer(Duration::from_millis(1)).await;
        started.cancel();
        let _ = started.result().await;
        Ok(())
    }
}

#[tokio::test]
async fn event_groups_nexus_operation_cancel() {
    let wf_name = "event_groups_nexus_commands";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.set_core_task_types(WorkerTaskTypes {
        enable_workflows: true,
        enable_local_activities: false,
        enable_remote_activities: false,
        enable_nexus: true,
    });
    starter
        .sdk_config
        .register_workflow::<NexusCommandsWf>()
        .unwrap();
    let mut worker = starter.worker().await;
    let core_worker = starter.get_core_worker().await;
    let endpoint = mk_nexus_endpoint(&mut starter).await;
    worker
        .submit_workflow(
            NexusCommandsWf::run,
            endpoint,
            starter.workflow_options.clone(),
        )
        .await
        .unwrap();

    let mut starts = 0;
    tokio::select! {
        result = worker.run_until_done() => result.unwrap(),
        _ = async {
            loop {
                match core_worker.poll_nexus_task().await {
                    Ok(task) => {
                        let nt = task.unwrap_task();
                        let is_cancel = matches!(
                            nt.request.as_ref().and_then(|req| req.variant.as_ref()),
                            Some(request::Variant::CancelOperation(_))
                        );
                        let status = if is_cancel {
                            nexus_task_completion::Status::Completed(nexus::v1::Response {
                                variant: Some(nexus::v1::response::Variant::CancelOperation(
                                    CancelOperationResponse {},
                                )),
                            })
                        } else {
                            starts += 1;
                            let start_variant = if starts == 1 {
                                start_operation_response::Variant::SyncSuccess(
                                    start_operation_response::Sync {
                                        payload: Some("ok".into()),
                                        links: vec![],
                                    },
                                )
                            } else {
                                start_operation_response::Variant::AsyncSuccess(
                                    #[allow(deprecated)]
                                    start_operation_response::Async {
                                        operation_id: "op-1".to_string(),
                                        links: vec![],
                                        operation_token: "op-1".to_string(),
                                    },
                                )
                            };
                            nexus_task_completion::Status::Completed(nexus::v1::Response {
                                variant: Some(nexus::v1::response::Variant::StartOperation(
                                    StartOperationResponse {
                                        variant: Some(start_variant),
                                    },
                                )),
                            })
                        };
                        core_worker
                            .complete_nexus_task(NexusTaskCompletion {
                                task_token: nt.task_token,
                                status: Some(status),
                            })
                            .await
                            .unwrap();
                    }
                    Err(PollError::ShutDown) => break,
                    Err(err) => panic!("nexus poll failed: {err:?}"),
                }
            }
        } => {}
    }

    let history = starter.get_history().await;
    let both = set(["scope-id", "direct-id"]);
    let scheduled: Vec<_> = history
        .events
        .iter()
        .filter(|e| e.event_type() == EventType::NexusOperationScheduled)
        .collect();
    assert_eq!(scheduled.len(), 2);
    for event in scheduled {
        assert_eq!(label_ids(event), both);
    }
    let cancel = history
        .events
        .iter()
        .find(|e| e.event_type() == EventType::NexusOperationCancelRequested)
        .expect("nexus cancel requested");
    assert_eq!(label_ids(cancel), both);
}

#[workflow]
#[derive(Default)]
struct ContinueAsNewCommandsWf;

#[workflow_methods]
impl ContinueAsNewCommandsWf {
    #[run(name = "event_groups_continue_as_new")]
    async fn run(ctx: &mut WorkflowContext<Self>, second_run: bool) -> WorkflowResult<()> {
        if second_run {
            return Ok(());
        }
        let scope = EventGroup::with_id("scope-label", "scope-id");
        let direct = EventGroup::with_id("direct-label", "direct-id");
        ctx.with_event_group(scope).continue_as_new(
            true,
            ContinueAsNewOptions::builder()
                .event_groups(vec![direct])
                .build(),
        )?;
        Ok(())
    }
}

#[tokio::test]
async fn event_groups_continue_as_new() {
    let wf_name = "event_groups_continue_as_new";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .sdk_config
        .register_workflow::<ContinueAsNewCommandsWf>()
        .unwrap();
    let mut worker = starter.worker().await;
    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            ContinueAsNewCommandsWf::run,
            false,
            WorkflowStartOptions::new(task_queue, starter.get_wf_id().to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let events = handle
        .fetch_history(Default::default())
        .into_events()
        .await
        .unwrap();
    let continued = events
        .iter()
        .find(|e| e.event_type() == EventType::WorkflowExecutionContinuedAsNew)
        .expect("continued-as-new");
    assert_eq!(label_ids(continued), set(["scope-id", "direct-id"]));
}

fn timer_duration_millis(
    event: &temporalio_common::protos::temporal::api::history::v1::HistoryEvent,
) -> Option<i64> {
    match event.attributes.as_ref()? {
        history_event::Attributes::TimerStartedEventAttributes(attrs) => {
            let duration = attrs.start_to_fire_timeout.as_ref()?;
            Some(duration.seconds * 1000 + i64::from(duration.nanos / 1_000_000))
        }
        _ => None,
    }
}

fn local_activity_by_id<'a>(history: &'a History, activity_id: &str) -> &'a HistoryEvent {
    local_activities_by_id(history, activity_id)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("local activity {activity_id}"))
}

fn local_activities_by_id<'a>(history: &'a History, activity_id: &str) -> Vec<&'a HistoryEvent> {
    history
        .events
        .iter()
        .filter(|event| local_activity_id(event).as_deref() == Some(activity_id))
        .collect()
}

fn local_activity_id(event: &HistoryEvent) -> Option<String> {
    match event.attributes.as_ref()? {
        history_event::Attributes::MarkerRecordedEventAttributes(attrs)
            if attrs.marker_name == "core_local_activity" =>
        {
            extract_local_activity_marker_data(&attrs.details).map(|data| data.activity_id)
        }
        _ => None,
    }
}

fn search_attribute_keys(
    event: &temporalio_common::protos::temporal::api::history::v1::HistoryEvent,
) -> HashSet<String> {
    match event.attributes.as_ref() {
        Some(history_event::Attributes::UpsertWorkflowSearchAttributesEventAttributes(attrs)) => {
            attrs
                .search_attributes
                .as_ref()
                .map(|sa| sa.indexed_fields.keys().cloned().collect())
                .unwrap_or_default()
        }
        _ => HashSet::new(),
    }
}

fn label_marker(id: &str, label: &str) -> EventGroupMarker {
    EventGroupMarker {
        variant: Some(Variant::Label(Label {
            id: id.to_string(),
            label: Some(label.as_json_payload().unwrap()),
        })),
    }
}

fn derived_group_id(original_execution_run_id: &str, label: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(original_execution_run_id.as_bytes());
    hasher.update(label.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn original_execution_run_id(history: &History) -> &str {
    history
        .events
        .iter()
        .find_map(|event| match event.attributes.as_ref()? {
            history_event::Attributes::WorkflowExecutionStartedEventAttributes(attrs) => {
                Some(attrs.original_execution_run_id.as_str())
            }
            _ => None,
        })
        .expect("WorkflowExecutionStarted")
}

fn activities_by_input(
    history: &History,
) -> std::collections::HashMap<
    String,
    &temporalio_common::protos::temporal::api::history::v1::HistoryEvent,
> {
    history
        .events
        .iter()
        .filter(|event| event.event_type() == EventType::ActivityTaskScheduled)
        .filter_map(|event| {
            let attrs = match event.attributes.as_ref()? {
                history_event::Attributes::ActivityTaskScheduledEventAttributes(attrs) => attrs,
                _ => return None,
            };
            let payload = attrs.input.as_ref()?.payloads.first()?;
            let name = String::from_utf8(payload.data.clone()).ok()?;
            let name = name.trim_matches('"').to_string();
            Some((name, event))
        })
        .collect()
}

fn label_ids(
    event: &temporalio_common::protos::temporal::api::history::v1::HistoryEvent,
) -> HashSet<String> {
    event
        .event_group_markers
        .iter()
        .filter_map(|marker| match marker.variant.as_ref()? {
            Variant::Label(label) => Some(label.id.clone()),
            _ => None,
        })
        .collect()
}

fn inbound_event_id(
    event: &temporalio_common::protos::temporal::api::history::v1::HistoryEvent,
) -> Option<i64> {
    event
        .event_group_markers
        .iter()
        .find_map(|marker| match marker.variant.as_ref()? {
            Variant::InboundEvent(inbound) => Some(inbound.inbound_event_id),
            _ => None,
        })
}

fn inbound_update_id(
    event: &temporalio_common::protos::temporal::api::history::v1::HistoryEvent,
) -> Option<String> {
    event
        .event_group_markers
        .iter()
        .find_map(|marker| match marker.variant.as_ref()? {
            Variant::InboundUpdate(inbound) => Some(inbound.inbound_update_id.clone()),
            _ => None,
        })
}

fn signal_name(
    event: &temporalio_common::protos::temporal::api::history::v1::HistoryEvent,
) -> Option<&str> {
    match event.attributes.as_ref()? {
        history_event::Attributes::WorkflowExecutionSignaledEventAttributes(attrs) => {
            Some(attrs.signal_name.as_str())
        }
        _ => None,
    }
}

fn set<T: Into<String>, const N: usize>(ids: [T; N]) -> HashSet<String> {
    ids.into_iter().map(Into::into).collect()
}
