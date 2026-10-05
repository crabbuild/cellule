use super::*;
use cellule_runtime::fleet::operations::{
    AttemptEvent, AttemptId, AttemptPhase, EnrollmentRole, EnrollmentStatus, FleetAction,
    FleetActionOutcome, FleetOutcome, MoveAttempt, MovementAction, ReceiverRoute,
};
use cellule_runtime::fleet::placement::{PlacementEligibility, PlacementPlanner};
use cellule_runtime::identity::{NodeId, SessionId};
use cellule_runtime::node::NodeMode;
use std::collections::HashSet;

struct RetainedMovementAcceptance {
    hops: usize,
    acceptance: super::super::FleetActionAcceptance,
}

enum ReceiverDispatchChoice {
    Direct,
    Routed(FleetAction),
    Blocked,
}

impl FleetReconciler {
    pub(super) async fn advance(
        &self,
        id: AttemptId,
        clock: &PassClock<'_>,
        report: &mut FleetReconcileReport,
    ) -> Result<()> {
        let attempt = current(report, id)?.clone();
        let mut effect = attempt.next_action();
        if effect == MovementAction::Retire {
            if matches!(
                attempt.phase(),
                AttemptPhase::Activated | AttemptPhase::Recovered
            ) {
                let fresh = self.inspect_attempt(&attempt, clock, report).await?;
                match &fresh.outcome {
                    FleetOutcome::Activated(evidence) => {
                        attempt.validate_activation(evidence).map_err(operation)?
                    }
                    FleetOutcome::Recovered(evidence)
                        if attempt.recovered().is_some_and(|original| {
                            evidence.recovery == original.recovery
                                && evidence.serving.position.epoch
                                    >= original.serving.position.epoch
                        }) => {}
                    _ => {
                        report.blocked(DrainBlocker::OutcomeUnknown);
                        return Ok(());
                    }
                }
            }
            let progress = report
                .snapshot
                .head()
                .retirement_page(&[id])
                .map_err(operation)?;
            self.commit(clock, report, JournalTransition::Retire { progress })
                .await?;
            report.retired += 1;
            return Ok(());
        }
        let now = clock.now()?;
        let transition = match attempt.phase() {
            AttemptPhase::Planned if now >= attempt.spec().deadline_ms => {
                effect = MovementAction::Cancel;
                Some(AttemptEvent::BeginCancel)
            }
            AttemptPhase::Planned => Some(AttemptEvent::BeginPrepare),
            AttemptPhase::Reserved
                if attempt.blocker().is_some()
                    || now >= attempt.spec().deadline_ms
                    || attempt.reservation().is_none_or(|r| r.expires_at_ms <= now) =>
            {
                effect = MovementAction::Cancel;
                Some(AttemptEvent::BeginCancel)
            }
            AttemptPhase::Reserved
                if report.snapshot.head().maintenance().is_some_and(|m| {
                    m.id() == id.operation && m.node() == attempt.spec().source_node
                }) =>
            {
                effect = MovementAction::ReleaseMaintenance;
                Some(AttemptEvent::BeginMaintenanceRelease)
            }
            AttemptPhase::Reserved => Some(AttemptEvent::BeginRelease),
            AttemptPhase::Released
                if !attempt.receiver_resources_settled()
                    && attempt
                        .reservation()
                        .is_some_and(|r| r.expires_at_ms <= now) =>
            {
                // Expired unused credit must join before first activation.
                // The exact release and fleet permits remain charged.
                effect = MovementAction::Cancel;
                Some(AttemptEvent::BeginCancel)
            }
            AttemptPhase::Released => Some(AttemptEvent::BeginActivate),
            _ => None,
        };
        if let Some(event) = transition {
            self.commit_event(id, event, clock, report).await?;
        } else if effect == MovementAction::Inspect {
            let outcome = match self.inspect_attempt(&attempt, clock, report).await {
                Ok(outcome) => outcome,
                Err(error)
                    if attempt.phase() == AttemptPhase::Activating
                        && attempt.released().is_some()
                        && matches!(
                            &error,
                            Error::Facility {
                                name: "fleet-transport",
                                ..
                            }
                        ) =>
                {
                    // A stopped receiver cannot answer inspection. Only a
                    // fresh typed closure and committed route can authorize a
                    // continuation. Canonical claims still require the
                    // receiver executor's ordinary ownership checks.
                    let choice = match self
                        .receiver_dispatch_action(&attempt, MovementAction::Activate, clock, report)
                        .await
                    {
                        Ok(choice) => choice,
                        Err(route_error) => {
                            report.failed(id, error);
                            return Err(route_error);
                        }
                    };
                    let ReceiverDispatchChoice::Routed(action) = choice else {
                        return Err(error);
                    };
                    report.failed(id, error);
                    return self
                        .dispatch_action(&attempt, MovementAction::Activate, &action, clock, report)
                        .await;
                }
                Err(error) => return Err(error),
            };
            if attempt.phase() == AttemptPhase::Preparing
                && clock.now()? >= attempt.spec().deadline_ms
            {
                self.commit_event(id, AttemptEvent::BeginCancel, clock, report)
                    .await?;
                return self
                    .dispatch_effect(id, MovementAction::Cancel, clock, report)
                    .await;
            }
            if !matches!(
                outcome.outcome,
                FleetOutcome::Unknown | FleetOutcome::Blocked(_) | FleetOutcome::Rejected(_)
            ) {
                return self.consume(id, &outcome, true, clock, report).await;
            }
            // The exact acceptance decides whether replay can inspect owned
            // work. A lookup alone cannot fence a concurrent delayed acceptance.
            effect = phase_effect(attempt.phase())
                .ok_or(Error::Control("fleet inspection phase has no effect"))?;
            let original = self.retained_action(&attempt, effect, clock).await?;
            if original.is_none() {
                self.commit(
                    clock,
                    report,
                    JournalTransition::ResolveUnaccepted { id, effect },
                )
                .await?;
                let resolved = current(report, id)?;
                if resolved.phase() == AttemptPhase::Reserved {
                    report.blocked(DrainBlocker::Deadline);
                    return Ok(());
                }
                effect = phase_effect(resolved.phase())
                    .ok_or(Error::Control("resolved absence has no effect"))?;
                return self.dispatch_effect(id, effect, clock, report).await;
            }
            if let Some(super::super::FleetActionAcceptance::Existing {
                result: Some(result),
                ..
            }) = &original
            {
                // Current serving still needs fresh capture; retained source
                // release/refusal/cleanup is historical and may advance directly.
                if !matches!(
                    result.outcome,
                    FleetOutcome::Activated(_) | FleetOutcome::Recovered(_) | FleetOutcome::Unknown
                ) {
                    return self.consume(id, result, false, clock, report).await;
                }
                if matches!(
                    result.outcome,
                    FleetOutcome::Activated(_) | FleetOutcome::Recovered(_)
                ) {
                    report.blocked(DrainBlocker::OutcomeUnknown);
                    return Ok(());
                }
            }
            if effect.is_source_release() && original.is_some() {
                // A source effect without retained release evidence cannot be
                // repeated or declared refused. Canonical recovery is separate.
                report.blocked(DrainBlocker::OutcomeUnknown);
                return Ok(());
            }
            if matches!(
                outcome.outcome,
                FleetOutcome::Blocked(DrainBlocker::Deadline)
            ) && matches!(
                attempt.phase(),
                AttemptPhase::Preparing | AttemptPhase::Activating
            ) {
                self.commit_event(id, AttemptEvent::BeginCancel, clock, report)
                    .await?;
                effect = MovementAction::Cancel;
            }
        }
        self.dispatch_effect(id, effect, clock, report).await
    }

    async fn dispatch_effect(
        &self,
        id: AttemptId,
        effect: MovementAction,
        clock: &PassClock<'_>,
        report: &mut FleetReconcileReport,
    ) -> Result<()> {
        let attempt = current(report, id)?.clone();
        // No preparation was accepted for an expired Planned attempt. Its
        // cancellation is journal-local; a missing receipt alone would not
        // justify this shortcut for Preparing or any later phase.
        if effect == MovementAction::Cancel
            && attempt.reservation().is_none()
            && !matches!(attempt.phase(), AttemptPhase::CleaningReceiver)
        {
            let accepted = self
                .retained_action(&attempt, MovementAction::Prepare, clock)
                .await?;
            if accepted.is_none() {
                return self
                    .commit_event(id, AttemptEvent::Cancelled, clock, report)
                    .await;
            }
        }
        let action = if matches!(effect, MovementAction::Activate | MovementAction::Cancel) {
            match self
                .receiver_dispatch_action(&attempt, effect, clock, report)
                .await?
            {
                ReceiverDispatchChoice::Routed(action) => action,
                ReceiverDispatchChoice::Direct => {
                    if attempt.blocker() == Some(DrainBlocker::OutcomeUnknown) {
                        let Some(super::super::FleetActionAcceptance::Existing {
                            accepted, ..
                        }) = self.retained_action(&attempt, effect, clock).await?
                        else {
                            report.blocked(DrainBlocker::OutcomeUnknown);
                            return Ok(());
                        };
                        accepted.action().clone()
                    } else {
                        report
                            .snapshot
                            .head()
                            .movement_action(id, effect, clock.now()?)
                            .map_err(operation)?
                    }
                }
                ReceiverDispatchChoice::Blocked => return Ok(()),
            }
        } else if attempt.blocker() == Some(DrainBlocker::OutcomeUnknown) {
            // Unknown does not authorize a new effect. Replay only the exact
            // original acceptance; the node may inspect/resume its owned work.
            let Some(super::super::FleetActionAcceptance::Existing { accepted, .. }) =
                self.retained_action(&attempt, effect, clock).await?
            else {
                report.blocked(DrainBlocker::OutcomeUnknown);
                return Ok(());
            };
            accepted.action().clone()
        } else {
            report
                .snapshot
                .head()
                .movement_action(id, effect, clock.now()?)
                .map_err(operation)?
        };
        self.dispatch_action(&attempt, effect, &action, clock, report)
            .await
    }

    async fn dispatch_action(
        &self,
        attempt: &MoveAttempt,
        effect: MovementAction,
        action: &FleetAction,
        clock: &PassClock<'_>,
        report: &mut FleetReconcileReport,
    ) -> Result<()> {
        let id = attempt.spec().id;
        report.dispatched += 1;
        let completion = call(
            clock.deadline,
            "fleet-transport",
            self.transport.dispatch(action, clock.deadline),
        )
        .await?;
        let (node, session) = if effect.is_source_release() {
            endpoint(attempt, effect)
        } else {
            action
                .receiver_endpoint()
                .ok_or(Error::Control("fleet receiver action has no endpoint"))?
        };
        completion
            .accepted
            .validate_replay(action, node, session)
            .map_err(operation)?;
        completion
            .accepted
            .validate_result(&completion.outcome)
            .map_err(operation)?;
        if !completion.committed {
            if let Some(error) = &completion.journal_error {
                tracing::warn!(error = ?error, "fleet result publication remains unresolved");
            }
            report.blocked(DrainBlocker::PendingPublication);
            return Ok(());
        }
        if matches!(
            completion.outcome.outcome,
            FleetOutcome::Activated(_) | FleetOutcome::Recovered(_)
        ) {
            let fresh = self.inspect_attempt(attempt, clock, report).await?;
            self.consume(id, &fresh, true, clock, report).await
        } else {
            self.consume(id, &completion.outcome, false, clock, report)
                .await
        }
    }

    async fn receiver_dispatch_action(
        &self,
        attempt: &MoveAttempt,
        effect: MovementAction,
        clock: &PassClock<'_>,
        report: &mut FleetReconcileReport,
    ) -> Result<ReceiverDispatchChoice> {
        let retained = self.retained_action(attempt, effect, clock).await?;
        let accepted = match &retained {
            Some(super::super::FleetActionAcceptance::Existing { accepted, .. }) => {
                Some(accepted.action().clone())
            }
            _ => None,
        };

        // A durable terminal result always wins over a new route. In particular,
        // process closure alone cannot prove that a receiver did not publish a
        // Serving control immediately before it stopped.
        if matches!(
            &retained,
            Some(super::super::FleetActionAcceptance::Existing {
                result: Some(result),
                ..
            }) if matches!(
                &result.outcome,
                FleetOutcome::Activated(_)
                    | FleetOutcome::Recovered(_)
                    | FleetOutcome::ReceiverCleaned
            )
        ) {
            return Ok(accepted.map_or(
                ReceiverDispatchChoice::Direct,
                ReceiverDispatchChoice::Routed,
            ));
        }

        let route_source = if effect == MovementAction::Cancel {
            match self
                .retained_action(attempt, MovementAction::Activate, clock)
                .await?
            {
                Some(super::super::FleetActionAcceptance::Existing { accepted, .. }) => {
                    accepted.action().receiver_route().cloned()
                }
                _ => None,
            }
        } else {
            None
        };
        let current_route = accepted
            .as_ref()
            .and_then(|action| action.receiver_route().cloned())
            .or(route_source);
        let previous = current_route.as_ref().map_or(
            (attempt.spec().destination_node, attempt.spec().destination),
            |route| route.target(attempt.spec()),
        );

        let roster = crate::fleet::FleetRoster::collect(
            self.journal.as_ref(),
            &report.snapshot,
            clock.deadline,
        )
        .await?;
        let observation = call(
            clock.deadline,
            "fleet-observer",
            self.observer.observe(&roster, clock.now()?, clock.deadline),
        )
        .await?;
        if observation.scope != self.scope || observation.registry != report.snapshot.registry() {
            return Err(operation(OperationError::Conflict));
        }
        roster
            .confirm(self.journal.as_ref(), clock.deadline)
            .await?;
        let observation = observation.with_roster(roster)?;
        let now = clock.now()?;
        let placements = observation.placements(now)?;
        let Some(closures) = observation.failed_boot_closures() else {
            if effect == MovementAction::Cancel && current_route.is_some() && accepted.is_none() {
                report.blocked(DrainBlocker::OutcomeUnknown);
                return Ok(ReceiverDispatchChoice::Blocked);
            }
            return Ok(accepted.map_or(
                ReceiverDispatchChoice::Direct,
                ReceiverDispatchChoice::Routed,
            ));
        };
        let closure_for = |endpoint| {
            closures.iter().find(|closure| {
                let target = closure.boot().spec().target;
                (target.node, target.session) == endpoint
                    && closure.boot().status() == EnrollmentStatus::Retired
                    && closure.snapshot() == &report.snapshot
            })
        };

        // An existing route continues to replay at its current endpoint while
        // that boot remains live. A routed Cancel still needs a fresh typed
        // closure for the original handoff in the same head/registry barrier.
        if current_route
            .as_ref()
            .is_some_and(|route| route.target(attempt.spec()) == previous)
            && closure_for(previous).is_none()
        {
            if effect == MovementAction::Activate {
                return Ok(accepted.map_or(
                    ReceiverDispatchChoice::Blocked,
                    ReceiverDispatchChoice::Routed,
                ));
            }
            if accepted
                .as_ref()
                .is_some_and(|action| action.receiver_route().is_some())
            {
                return Ok(ReceiverDispatchChoice::Routed(accepted.ok_or(
                    Error::Control("routed cancellation acceptance is absent"),
                )?));
            }
            let Some(route) = current_route else {
                return Ok(accepted.map_or(
                    ReceiverDispatchChoice::Direct,
                    ReceiverDispatchChoice::Routed,
                ));
            };
            let Some(handoff) = route.latest_handoff() else {
                return Err(operation(OperationError::Conflict));
            };
            let Some(closure) = closure_for(handoff.previous()) else {
                report.blocked(DrainBlocker::OutcomeUnknown);
                return Ok(ReceiverDispatchChoice::Blocked);
            };
            if route.registry() != Some(report.snapshot.registry()) {
                report.blocked(DrainBlocker::StaleObservation);
                return Ok(ReceiverDispatchChoice::Blocked);
            }
            return self
                .accept_routed_receiver_action(attempt, effect, route, closure, clock, report)
                .await;
        }

        let Some(closure) = closure_for(previous) else {
            return Ok(accepted.map_or(
                ReceiverDispatchChoice::Direct,
                ReceiverDispatchChoice::Routed,
            ));
        };
        let roster = observation
            .roster()
            .ok_or(Error::Control("fleet roster was not retained"))?;
        if !roster.covers_advertisements(&observation.nodes, now)? {
            report.blocked(DrainBlocker::IncompleteObservation);
            return Ok(ReceiverDispatchChoice::Blocked);
        }

        let mut projected = placements;
        for other in report.snapshot.head().attempts() {
            if other.spec().id == attempt.spec().id {
                continue;
            }
            let mut endpoint = (other.spec().destination_node, other.spec().destination);
            if let Some(super::super::FleetActionAcceptance::Existing { accepted, .. }) = self
                .retained_action(other, MovementAction::Activate, clock)
                .await?
                && let Some(routed) = accepted.action().receiver_endpoint()
            {
                endpoint = routed;
            }
            if let Some(receiver) = projected
                .iter_mut()
                .find(|node| (node.node, node.session) == endpoint)
            {
                receiver.free_memory_bytes = receiver
                    .free_memory_bytes
                    .saturating_sub(other.spec().cost.memory_bytes);
                receiver.free_disk_bytes = receiver
                    .free_disk_bytes
                    .saturating_sub(other.spec().cost.disk_bytes);
                receiver.active_cells = receiver.active_cells.saturating_add(1);
                receiver.running_jobs = receiver
                    .running_jobs
                    .saturating_add(other.spec().cost.job_credits);
            }
        }

        let planner = PlacementPlanner::default();
        let ranked = planner.rank(attempt.spec().target.cell_id(), now, &projected)?;
        let mut visited =
            HashSet::from([attempt.spec().source_node, attempt.spec().destination_node]);
        if let Some(route) = &current_route
            && let Some(handoff) = route.latest_handoff()
        {
            visited.insert(handoff.target().0);
        }
        let needs_capacity = effect == MovementAction::Activate;
        let roster = observation
            .roster()
            .ok_or(Error::Control("fleet roster was not retained"))?;
        let candidate = ranked.into_iter().find(|score| {
            let usable = if needs_capacity {
                score.eligibility == PlacementEligibility::Eligible
            } else {
                !matches!(
                    score.eligibility,
                    PlacementEligibility::Unauthenticated | PlacementEligibility::Stale
                )
            };
            if !usable || visited.contains(&score.node) {
                return false;
            }
            let Some(intent) = roster
                .intents()
                .iter()
                .find(|intent| intent.node() == score.node)
            else {
                return false;
            };
            if intent.session() != score.session || intent.mode() != NodeMode::Active {
                return false;
            }
            let enrolled = roster.enrollments().iter().any(|row| {
                row.status() == EnrollmentStatus::Established
                    && row.spec().target.node == score.node
                    && row.spec().target.session == score.session
                    && matches!(row.spec().role, EnrollmentRole::Node { .. })
            });
            if !enrolled {
                return false;
            }
            !needs_capacity
                || projected
                    .iter()
                    .find(|node| node.node == score.node && node.session == score.session)
                    .is_some_and(|node| {
                        node.free_memory_bytes >= attempt.spec().cost.memory_bytes
                            && node.free_disk_bytes >= attempt.spec().cost.disk_bytes
                            && node.max_active_cells.saturating_sub(node.active_cells) >= 1
                            && node.job_capacity.saturating_sub(node.running_jobs)
                                >= attempt.spec().cost.job_credits
                    })
        });
        let Some(candidate) = candidate else {
            report.blocked(if needs_capacity {
                DrainBlocker::ReceiverCapacity
            } else {
                DrainBlocker::IncompleteObservation
            });
            return Ok(ReceiverDispatchChoice::Blocked);
        };

        let route = match &current_route {
            Some(route) => route
                .extend(
                    self.scope,
                    attempt.spec(),
                    candidate.node,
                    candidate.session,
                    closure.digest(),
                    report.snapshot.registry(),
                )
                .map_err(operation)?,
            None => ReceiverRoute::begin(
                self.scope,
                attempt.spec(),
                candidate.node,
                candidate.session,
                closure.digest(),
                report.snapshot.registry(),
            )
            .map_err(operation)?,
        };
        self.accept_routed_receiver_action(attempt, effect, route, closure, clock, report)
            .await
    }

    async fn accept_routed_receiver_action(
        &self,
        attempt: &MoveAttempt,
        effect: MovementAction,
        route: ReceiverRoute,
        closure: &crate::fleet::FleetFailedBootClosure,
        clock: &PassClock<'_>,
        report: &FleetReconcileReport,
    ) -> Result<ReceiverDispatchChoice> {
        let action = report
            .snapshot
            .head()
            .movement_action_with_receiver_route(attempt.spec().id, effect, route, clock.now()?)
            .map_err(operation)?;
        let (node, session) = action
            .receiver_endpoint()
            .ok_or(Error::Control("routed receiver action has no endpoint"))?;
        let acceptance = call(
            clock.deadline,
            "fleet-journal",
            self.journal.accept_closed_receiver_action(
                &action,
                node,
                session,
                clock.now()?,
                closure,
            ),
        )
        .await?;
        let accepted = match acceptance {
            super::super::FleetActionAcceptance::New(accepted)
            | super::super::FleetActionAcceptance::Existing { accepted, .. } => accepted,
        };
        accepted
            .validate_replay(&action, node, session)
            .map_err(operation)?;
        Ok(ReceiverDispatchChoice::Routed(accepted.action().clone()))
    }

    async fn inspect_attempt(
        &self,
        attempt: &MoveAttempt,
        clock: &PassClock<'_>,
        report: &mut FleetReconcileReport,
    ) -> Result<FleetActionOutcome> {
        let (mut node, mut session) = endpoint(
            attempt,
            if matches!(
                attempt.phase(),
                AttemptPhase::Releasing | AttemptPhase::MaintenanceReleasing
            ) {
                if attempt.phase() == AttemptPhase::MaintenanceReleasing {
                    MovementAction::ReleaseMaintenance
                } else {
                    MovementAction::Release
                }
            } else {
                MovementAction::Activate
            },
        );
        let discover = attempt.released().is_some()
            && (attempt.phase() == AttemptPhase::Activating
                || (attempt.phase() == AttemptPhase::Activated
                    && attempt.receiver_resources_settled()));
        if discover && let Some(serving) = attempt.activated() {
            (node, session) = (serving.node, serving.session);
        }
        // Keep the ordinary direct check. Only unresolved serving needs a
        // fleet traversal, with time reserved for its capture and native read.
        let preferred_clock = clock.partition(2);
        let original = self
            .capture_attempt(
                attempt,
                node,
                session,
                if discover { &preferred_clock } else { clock },
                report,
            )
            .await;
        if !discover
            || matches!(&original, Ok(outcome) if matches!(outcome.outcome, FleetOutcome::Activated(_)))
        {
            return original;
        }
        let fallback = async {
            let Some(successor) = self.successor_endpoint(attempt, clock, report).await? else {
                return Ok(None);
            };
            if successor == (node, session) {
                return Ok(None);
            }
            let fresh = self
                .capture_attempt(attempt, successor.0, successor.1, clock, report)
                .await?;
            Ok::<_, Error>(matches!(fresh.outcome, FleetOutcome::Activated(_)).then_some(fresh))
        }
        .await;
        match fallback {
            Ok(Some(fresh)) => {
                if let Err(error) = original {
                    // Preserve the failed original endpoint even when another
                    // boot proves serving. No failure can free receiver credit.
                    report.failed(attempt.spec().id, error);
                }
                Ok(fresh)
            }
            Ok(None) => original,
            Err(error) => match original {
                Err(original) => {
                    tracing::warn!(error = ?error, "fleet successor fallback failed after original inspection failure");
                    Err(original)
                }
                Ok(_) => Err(error),
            },
        }
    }

    async fn capture_attempt(
        &self,
        attempt: &MoveAttempt,
        node: NodeId,
        session: SessionId,
        clock: &PassClock<'_>,
        report: &mut FleetReconcileReport,
    ) -> Result<FleetActionOutcome> {
        // Collecting a routing hint creates no effect acceptance. The request
        // still binds this pass's exact head, registry and capture interval.
        // Cleanup and its retained lookups continue using the original receiver.
        let action = report
            .snapshot
            .head()
            .movement_action(attempt.spec().id, MovementAction::Inspect, clock.now()?)
            .map_err(operation)?;
        let lease = report
            .snapshot
            .head()
            .controller()
            .ok_or_else(|| operation(OperationError::Fenced))?;
        let request = FleetInspectionRequest::new(
            action,
            report.snapshot.registry(),
            nonce(),
            node,
            session,
            clock.capture_deadline_ms()?.min(lease.expires_at_ms),
        )
        .map_err(operation)?;
        let observation = call(
            clock.deadline,
            "fleet-transport",
            self.transport.inspect(&request, clock.deadline),
        )
        .await?;
        observation
            .validate_for(&request, clock.now()?, 30_000)
            .map_err(operation)?;
        report.inspected += 1;
        Ok(observation.outcome().clone())
    }

    async fn retained_action(
        &self,
        attempt: &MoveAttempt,
        effect: MovementAction,
        clock: &PassClock<'_>,
    ) -> Result<Option<super::super::FleetActionAcceptance>> {
        let actions = call(
            clock.deadline,
            "fleet-journal",
            self.journal
                .load_movement_actions(self.scope, attempt, effect),
        )
        .await?;
        let mut selected: Option<RetainedMovementAcceptance> = None;
        for candidate in actions {
            let super::super::FleetActionAcceptance::Existing { accepted, result } = candidate
            else {
                return Err(operation(OperationError::Conflict));
            };
            accepted
                .validate_replay(accepted.action(), accepted.node(), accepted.session())
                .map_err(operation)?;
            let cellule_runtime::fleet::operations::FleetActionKind::Movement {
                action,
                attempt: input,
            } = accepted.action().kind()
            else {
                return Err(operation(OperationError::Conflict));
            };
            if accepted.action().scope() != self.scope
                || *action != effect
                || input.spec() != attempt.spec()
            {
                return Err(operation(OperationError::Conflict));
            }
            if let Some(result) = &result {
                accepted.validate_result(result).map_err(operation)?;
            }
            let route = accepted.action().receiver_route().cloned();
            let hops = route.as_ref().map_or(
                0,
                cellule_runtime::fleet::operations::ReceiverRoute::hop_count,
            );
            let candidate = RetainedMovementAcceptance {
                hops,
                acceptance: super::super::FleetActionAcceptance::Existing { accepted, result },
            };
            match &selected {
                None => selected = Some(candidate),
                Some(previous) if hops > previous.hops => {
                    selected = Some(candidate);
                }
                Some(previous) if hops < previous.hops => {}
                Some(_) => return Err(operation(OperationError::Conflict)),
            }
        }
        Ok(selected.map(|selected| selected.acceptance))
    }

    async fn commit_event(
        &self,
        id: AttemptId,
        event: AttemptEvent,
        clock: &PassClock<'_>,
        report: &mut FleetReconcileReport,
    ) -> Result<()> {
        let confirmed = match &event {
            AttemptEvent::Released(_) => Some(AttemptPhase::Released),
            AttemptEvent::Activated(_) => Some(AttemptPhase::Activated),
            AttemptEvent::Recovered(_) => Some(AttemptPhase::Recovered),
            AttemptEvent::Cancelled => Some(AttemptPhase::Cancelled),
            _ => None,
        };
        let revision = report.snapshot.head().revision();
        self.commit(clock, report, JournalTransition::Attempt { id, event })
            .await?;
        if report.snapshot.head().revision() != revision {
            match confirmed {
                Some(AttemptPhase::Released) => report.released += 1,
                Some(AttemptPhase::Activated) => report.activated += 1,
                Some(AttemptPhase::Recovered) => report.recovered += 1,
                Some(AttemptPhase::Cancelled) => report.cancelled += 1,
                _ => {}
            }
        }
        Ok(())
    }

    async fn consume(
        &self,
        id: AttemptId,
        result: &FleetActionOutcome,
        fresh: bool,
        clock: &PassClock<'_>,
        report: &mut FleetReconcileReport,
    ) -> Result<()> {
        let attempt = current(report, id)?.clone();
        if let FleetOutcome::Rejected(blocker) | FleetOutcome::Blocked(blocker) = &result.outcome {
            report.blocked(*blocker);
        }
        let event = match &result.outcome {
            FleetOutcome::Reserved(r) if attempt.phase() == AttemptPhase::Preparing => {
                if r.expires_at_ms <= clock.now()? {
                    self.commit_event(id, AttemptEvent::BeginCancel, clock, report)
                        .await?;
                    return Ok(());
                }
                AttemptEvent::Reserved(*r)
            }
            FleetOutcome::Released(p)
                if matches!(
                    attempt.phase(),
                    AttemptPhase::Releasing | AttemptPhase::MaintenanceReleasing
                ) =>
            {
                AttemptEvent::Released(p.clone())
            }
            FleetOutcome::Activated(e)
                if fresh
                    && matches!(
                        attempt.phase(),
                        AttemptPhase::Activating | AttemptPhase::CleaningReceiver
                    ) =>
            {
                AttemptEvent::Activated(e.clone())
            }
            FleetOutcome::Recovered(e) if fresh && attempt.phase() == AttemptPhase::Recovering => {
                AttemptEvent::Recovered(e.clone())
            }
            FleetOutcome::ReceiverCleaned if attempt.phase() == AttemptPhase::Cancelling => {
                AttemptEvent::Cancelled
            }
            FleetOutcome::ReceiverCleaned
                if matches!(
                    attempt.phase(),
                    AttemptPhase::Activated
                        | AttemptPhase::Recovered
                        | AttemptPhase::CleaningReceiver
                ) =>
            {
                AttemptEvent::ReceiverCleaned
            }
            FleetOutcome::Rejected(blocker)
                if matches!(
                    attempt.phase(),
                    AttemptPhase::Releasing | AttemptPhase::MaintenanceReleasing
                ) =>
            {
                AttemptEvent::ReleaseRefused(*blocker)
            }
            FleetOutcome::Rejected(_) if attempt.phase() == AttemptPhase::Preparing => {
                // Retained definite preparation refusal proves no receiver work
                // was installed, and this phase cannot yet have released source.
                self.commit_event(id, AttemptEvent::BeginCancel, clock, report)
                    .await?;
                AttemptEvent::Cancelled
            }
            FleetOutcome::Unknown => {
                report.blocked(DrainBlocker::OutcomeUnknown);
                return Ok(());
            }
            FleetOutcome::Blocked(blocker) | FleetOutcome::Rejected(blocker) => {
                report.blocked(*blocker);
                return Ok(());
            }
            _ => {
                report.blocked(DrainBlocker::OutcomeUnknown);
                return Ok(());
            }
        };
        self.commit_event(id, event, clock, report).await
    }
}

fn current(report: &FleetReconcileReport, id: AttemptId) -> Result<&MoveAttempt> {
    report
        .snapshot
        .head()
        .attempts()
        .iter()
        .find(|a| a.spec().id == id)
        .ok_or_else(|| operation(OperationError::NotFound))
}
fn phase_effect(phase: AttemptPhase) -> Option<MovementAction> {
    match phase {
        AttemptPhase::Preparing => Some(MovementAction::Prepare),
        AttemptPhase::Releasing => Some(MovementAction::Release),
        AttemptPhase::MaintenanceReleasing => Some(MovementAction::ReleaseMaintenance),
        AttemptPhase::Activating => Some(MovementAction::Activate),
        AttemptPhase::Recovering => Some(MovementAction::Recover),
        AttemptPhase::Cancelling
        | AttemptPhase::CleaningReceiver
        | AttemptPhase::Activated
        | AttemptPhase::Recovered => Some(MovementAction::Cancel),
        _ => None,
    }
}
fn endpoint(attempt: &MoveAttempt, effect: MovementAction) -> (NodeId, SessionId) {
    let spec = attempt.spec();
    if effect.is_source_release() {
        (spec.source_node, spec.source)
    } else {
        (spec.destination_node, spec.destination)
    }
}
