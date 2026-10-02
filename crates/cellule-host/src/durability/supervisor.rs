//! One canonical supervisor for automatic and requested epoch rotation.
use super::*;
use requests::{RotationRequests, RotationWork};

pub(super) async fn run_node_durability_supervisor<P: NodeDurabilityProvider>(
    provider: Arc<P>,
    runtime: CellRuntime,
    configuration: NodeDurabilitySupervisorConfig,
    session: SessionId,
    cancellation: CancellationToken,
    requests: Arc<RotationRequests>,
) -> FacilityResult {
    let mut recruit = tokio::time::interval(configuration.recruit_interval);
    let mut rotation = tokio::time::interval(configuration.rotation_interval);
    loop {
        if cancellation.is_cancelled() {
            return Ok(());
        }
        tokio::select! {
            () = cancellation.cancelled() => return Ok(()),
            _ = recruit.tick(), if runtime.node_durability().is_none() => {
                match provider.clone().recruit(configuration.limits, configuration.required_follower_bytes, configuration.live_node_limit).await {
                    Ok(Some(config)) => {
                        if config.identity().0 != session { return Err(Box::new(Error::Control("node-log enrollment boot differs from host"))); }
                        match config.build() {
                        Ok(durability) => {
                            if cancellation.is_cancelled() {
                                close_replacement(&provider, None, &durability, configuration.recruit_interval).await?;
                                return Ok(());
                            }
                            if let Err(error) = runtime.install_node_durability(configuration.application, Arc::clone(&durability)) {
                                provider.rotation_event(NodeDurabilityRotation::Failed);
                                close_replacement(&provider, None, &durability, configuration.recruit_interval).await?;
                                return Err(Box::new(error));
                            }
                        }
                        Err(_error) => provider.rotation_event(NodeDurabilityRotation::Failed),
                        }
                    },
                    Ok(None) => {}
                    Err(_) => provider.rotation_event(NodeDurabilityRotation::Failed),
                }
            }
            _ = rotation.tick(), if runtime.node_durability().is_some() => {
                rotate(Arc::clone(&provider), &runtime, configuration, &cancellation, &requests).await?;
            }
            () = requests.wake.notified() => {
                rotate(Arc::clone(&provider), &runtime, configuration, &cancellation, &requests).await?;
            }
        }
    }
}

async fn rotate<P: NodeDurabilityProvider>(
    provider: Arc<P>,
    runtime: &CellRuntime,
    configuration: NodeDurabilitySupervisorConfig,
    cancellation: &CancellationToken,
    requests: &RotationRequests,
) -> FacilityResult {
    let Some(work) = requests.claim(runtime, configuration.max_issued_frames)? else {
        return Ok(());
    };
    provider.rotation_event(NodeDurabilityRotation::Started);
    if let Some(record) = &work.record {
        record.phase(NodeLogRotationPhase::Retiring)?;
    }
    loop {
        let result = if let Some(record) = &work.record {
            work.durability
                .shutdown_for_maintenance()
                .await
                .and_then(|proof| record.retired(proof))
        } else {
            work.durability.shutdown().await
        };
        match result {
            Ok(()) => break,
            Err(Error::PendingPublication) => {
                provider.rotation_event(NodeDurabilityRotation::Pending);
                if !retry(cancellation, configuration.recruit_interval).await {
                    return Ok(());
                }
            }
            Err(error) => {
                provider.rotation_event(NodeDurabilityRotation::Failed);
                if let Some(record) = &work.record {
                    // Keep the claim and strict mode across retries. The normal
                    // timer cannot erase an unconfirmed maintenance obligation.
                    record.failed(Arc::new(error))?;
                    if !retry(cancellation, configuration.recruit_interval).await {
                        return Ok(());
                    }
                } else {
                    return Err(Box::new(error));
                }
            }
        }
    }
    if cancellation.is_cancelled() {
        return Ok(());
    }
    let replacement = loop {
        let result = provider
            .clone()
            .recruit(
                configuration.limits,
                configuration.required_follower_bytes,
                configuration.live_node_limit,
            )
            .await;
        match result {
            Ok(Some(config)) => {
                let identity = config.identity();
                let previous = work.durability.identity()?;
                if identity.0 != previous.0 || identity.1 != previous.1 || identity.2 <= previous.2
                {
                    report_failure(
                        &provider,
                        &work,
                        Box::new(Error::Control(
                            "node-log replacement identity or epoch differs",
                        )),
                    )?;
                    // Reject foreign scope before building or closing it. An
                    // application must reconcile any prior recruitment CAS.
                    if !retry(cancellation, configuration.recruit_interval).await {
                        return Ok(());
                    }
                    continue;
                }
                match config.build() {
                    Ok(durability) => break durability,
                    Err(error) => {
                        report_failure(&provider, &work, Box::new(error))?;
                    }
                }
            }
            Ok(None) => {}
            Err(error) => {
                report_failure(&provider, &work, error)?;
            }
        }
        if !retry(cancellation, configuration.recruit_interval).await {
            return Ok(());
        }
    };
    if cancellation.is_cancelled() {
        // Recruitment may have committed authority before cancellation. Join
        // its canonical close; a host deadline retains this supervisor's handle.
        close_replacement(
            &provider,
            work.record.as_deref(),
            &replacement,
            configuration.recruit_interval,
        )
        .await?;
        return Ok(());
    }
    let replacement_epoch = replacement.log_epoch()?;
    if replacement_epoch <= work.epoch {
        let error = Box::new(Error::Control("node-log replacement epoch did not advance"));
        let source = report_failure(&provider, &work, error)?;
        close_replacement(
            &provider,
            work.record.as_deref(),
            &replacement,
            configuration.recruit_interval,
        )
        .await?;
        return Err(Box::new(RetainedRotationError(source)));
    }
    match runtime.replace_node_durability(
        configuration.application,
        &work.durability,
        Arc::clone(&replacement),
    ) {
        Ok(_) => {
            requests.complete(&work, replacement_epoch)?;
            provider.rotation_event(NodeDurabilityRotation::Completed);
            Ok(())
        }
        Err(error) => {
            let source = report_failure(&provider, &work, Box::new(error))?;
            close_replacement(
                &provider,
                work.record.as_deref(),
                &replacement,
                configuration.recruit_interval,
            )
            .await?;
            Err(Box::new(RetainedRotationError(source)))
        }
    }
}

fn report_failure<P: NodeDurabilityProvider>(
    provider: &Arc<P>,
    work: &RotationWork,
    error: Box<dyn std::error::Error + Send + Sync>,
) -> cellule_runtime::Result<Arc<dyn std::error::Error + Send + Sync>> {
    provider.rotation_event(NodeDurabilityRotation::Failed);
    let error: Arc<dyn std::error::Error + Send + Sync> = Arc::from(error);
    if let Some(record) = &work.record {
        record.failed(Arc::clone(&error))?;
    }
    Ok(error)
}
#[derive(Debug)]
struct RetainedRotationError(Arc<dyn std::error::Error + Send + Sync>);
impl std::fmt::Display for RetainedRotationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for RetainedRotationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
async fn retry(cancellation: &CancellationToken, interval: Duration) -> bool {
    tokio::select! {
        () = cancellation.cancelled() => false,
        () = tokio::time::sleep(interval) => true,
    }
}

async fn close_replacement<P: NodeDurabilityProvider>(
    provider: &Arc<P>,
    record: Option<&requests::RotationRecord>,
    replacement: &Arc<cellule_runtime::node::durability::NodeDurability>,
    interval: Duration,
) -> FacilityResult {
    loop {
        match replacement.shutdown().await {
            Ok(()) => return Ok(()),
            Err(error) => {
                provider.rotation_event(NodeDurabilityRotation::Failed);
                if let Some(record) = record {
                    record.failed(Arc::new(error))?;
                }
                // Recruitment is already accepted. Work cancellation must not
                // discard this exact generation or its canonical cleanup join.
                tokio::time::sleep(interval).await;
            }
        }
    }
}
