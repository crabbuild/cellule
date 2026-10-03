use crate::{
    assembly::Service,
    emit,
    files::{self, Input, RequestFile},
    receipt,
};
use cellule_cookbook_support::{new_identity, now_ms};
use cellule_cookbook_telemetry_ingest::*;
use cellule_runtime::{primitives::queue::QueueSendOutcome, shard_for_scope};
use serde::{Deserialize, Serialize};
use std::{path::Path, time::Duration};
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    version: u8,
    devices: Vec<DeviceKey>,
    expected: Vec<DeviceState>,
    requests: Vec<RequestFile>,
}
impl Plan {
    fn validate(&self) -> Result<(), BoxError> {
        if self.version != 1
            || self.devices.len() != 2
            || self.devices[0] == self.devices[1]
            || self.expected.len() != 2
            || self.requests.len() != 4
        {
            return Err("invalid telemetry active demo plan".into());
        }
        for (key, expected) in self.devices.iter().zip(&self.expected) {
            expected.validate()?;
            if expected.device != *key {
                return Err("telemetry demo source binding differs".into());
            }
        }
        for record in &self.requests {
            record.validate()?;
            if record.tenant != "cookbook-demo" {
                return Err("telemetry demo plan tenant differs".into());
            }
        }
        for (index, record) in self.requests[..2].iter().enumerate() {
            let Input::Register { device, window } = &record.operation else {
                return Err("telemetry demo registration plan differs".into());
            };
            if *device != self.devices[index] || *window != self.expected[index].window {
                return Err("telemetry demo registration binding differs".into());
            }
        }
        let [
            Input::Submit { batch: first },
            Input::Submit { batch: second },
        ] = [&self.requests[2].operation, &self.requests[3].operation]
        else {
            return Err("telemetry demo submission plans differ".into());
        };
        if first.id == second.id || first.events != second.events || first.events.len() != 3 {
            return Err("telemetry demo repeat binding differs".into());
        }
        for event in &first.events {
            let state = self
                .expected
                .iter()
                .find(|v| v.device == event.device)
                .ok_or("telemetry demo event outside roster")?;
            if !state.events.iter().any(|v| v.event == *event) {
                return Err("telemetry demo expected source omits submitted event".into());
            }
        }
        Ok(())
    }
}
fn request(input: Input) -> Result<RequestFile, BoxError> {
    Ok(RequestFile {
        version: 1,
        tenant: "cookbook-demo".into(),
        identity: new_identity()?.into(),
        available_at_ms: now_ms()?,
        operation: input,
    })
}
async fn create(service: &Service) -> Result<Plan, BoxError> {
    let mut keys: Vec<DeviceKey> = vec![];
    for index in 0..64 {
        let key = DeviceKey::new(format!("device-{index}"))?;
        if keys.is_empty()
            || shard_for_scope(SUMMARIES, key.as_bytes(), 2)?
                != shard_for_scope(SUMMARIES, keys[0].as_bytes(), 2)?
        {
            keys.push(key);
        }
        if keys.len() == 2 {
            break;
        }
    }
    if keys.len() != 2 {
        return Err("bounded telemetry demo search did not find two summary shards".into());
    }
    let window = Window {
        start_ms: 120000,
        minutes: 16,
    };
    let mut expected = vec![];
    let mut requests = vec![];
    for key in &keys {
        let client = service.device(key.clone()).await?;
        let state = client.get(None).await?.output.unwrap_or(DeviceState {
            device: key.clone(),
            window,
            revision: 1,
            events: vec![],
            effect_id: [0; 32],
        });
        if state.window != window {
            return Err("telemetry demo device has a different immutable window".into());
        }
        expected.push(state);
        requests.push(request(Input::Register {
            device: key.clone(),
            window,
        })?);
    }
    let max_a = expected[0].events.last().map_or(0, |v| v.event.sequence);
    let max_b = expected[1].events.last().map_or(0, |v| v.event.sequence);
    let events = vec![
        Event {
            device: keys[0].clone(),
            sequence: max_a.checked_add(2).ok_or("demo sequence overflow")?,
            at_ms: 180500,
            value_milli: 20000,
        },
        Event {
            device: keys[0].clone(),
            sequence: max_a.checked_add(1).ok_or("demo sequence overflow")?,
            at_ms: 120500,
            value_milli: 10000,
        },
        Event {
            device: keys[1].clone(),
            sequence: max_b.checked_add(1).ok_or("demo sequence overflow")?,
            at_ms: 120500,
            value_milli: -5000,
        },
    ];
    for event in &events {
        event.validate()?;
        let state = expected
            .iter_mut()
            .find(|v| v.device == event.device)
            .ok_or("missing demo device")?;
        let reordered = state
            .events
            .last()
            .is_some_and(|v| v.event.sequence > event.sequence);
        state.events.push(RecordedEvent {
            event: event.clone(),
            reordered,
        });
        state.events.sort_by_key(|v| v.event.sequence);
        state.revision += 1;
        state.effect_id = [0; 32];
        state.validate()?;
    }
    for _ in 0..2 {
        requests.push(request(Input::Submit {
            batch: Batch {
                id: BatchId::parse(&uuid::Uuid::now_v7().to_string())?,
                events: events.clone(),
            },
        })?);
    }
    let plan = Plan {
        version: 1,
        devices: keys,
        expected,
        requests,
    };
    plan.validate()?;
    Ok(plan)
}
pub(crate) async fn run(service: &Service, state: &Path) -> Result<(), BoxError> {
    std::fs::create_dir_all(state)?;
    let active = state.join("demo-active.json");
    let plan = if active.exists() {
        let plan: Plan = files::load(&active)?;
        plan.validate()?;
        plan
    } else {
        let plan = create(service).await?;
        files::save(&active, &serde_json::to_vec(&plan)?)?;
        plan
    };
    emit(&serde_json::json!({"event":"demo_plan","path":active,"devices":plan.devices}))?;
    for record in &plan.requests[..2] {
        crate::apply(service, record).await?;
    }
    let producer = service.producer().await?;
    let mut messages = vec![];
    for record in &plan.requests[2..] {
        let Input::Submit { batch } = &record.operation else {
            return Err("invalid frozen telemetry submission".into());
        };
        let result = producer
            .send(record.identity.native()?, batch, record.available_at_ms)
            .await?;
        let QueueSendOutcome::Sent { message_id } = result.output else {
            return Err("telemetry demo producer binding conflict".into());
        };
        let id = MessageId::parse(&uuid::Uuid::from_bytes(message_id).to_string())?;
        messages.push(id);
        emit(
            &serde_json::json!({"event":"batch_submitted","batch":batch.id,"message":id,"receipt":receipt(result.receipt)}),
        )?;
    }
    let (tx, mut events) = tokio::sync::mpsc::channel(32);
    let (sx, mut summaries) = tokio::sync::mpsc::channel(32);
    spawn_consumers(
        &service.source,
        service.handle.clone(),
        &plan.devices,
        ConsumerOptions {
            progress: Some(tx),
            ..Default::default()
        },
    )
    .await?;
    let audit = service.audit().await?;
    let ingress_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        while let Ok(progress) = events.try_recv() {
            emit(&serde_json::json!({"event":"consumer","progress":progress}))?;
        }
        if !service.source.is_ready() {
            return Err("telemetry demo consumer failed readiness".into());
        }
        let mut complete = true;
        for id in &messages {
            complete &= audit.get(*id, None).await?.output.is_some();
        }
        let info = service
            .handle
            .queue::<Ingress>()?
            .info(0, None)
            .await?
            .output;
        complete &= info.ready == 0 && info.leased == 0 && info.dead == 0;
        if complete {
            break;
        }
        if tokio::time::Instant::now() >= ingress_deadline {
            return Err("telemetry demo ingress did not converge; preserve active plan".into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let selected = service
        .device(plan.devices[0].clone())
        .await?
        .get(None)
        .await?
        .output
        .ok_or("telemetry demo source disappeared")?;
    if selected.snapshot()? != plan.expected[0].snapshot()? {
        return Err("telemetry demo accepted source differs from frozen plan".into());
    }
    // Pin the pause to the latest intent after all source entries commit. Claim order never
    // chooses this interruption point, and source completion cannot precede its acknowledgment.
    spawn_delivery(
        &service.source,
        &service.summary,
        service.handle.clone(),
        service.receiver.clone(),
        &plan.devices,
        DeliveryOptions {
            controlled_effect: Some(selected.effect_id),
            after_publication: Duration::from_secs(10),
            drop_reply_once: true,
            progress: Some(sx),
            ..Default::default()
        },
    )
    .await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        while let Ok(progress) = events.try_recv() {
            emit(&serde_json::json!({"event":"consumer","progress":progress}))?;
        }
        while let Ok(progress) = summaries.try_recv() {
            emit(&serde_json::json!({"event":"summary","progress":progress}))?;
        }
        if !service.source.is_ready() || !service.summary.is_ready() {
            return Err("telemetry demo worker failed readiness".into());
        }
        let mut done = true;
        for id in &messages {
            done &= audit.get(*id, None).await?.output.is_some();
        }
        for (key, expected) in plan.devices.iter().zip(&plan.expected) {
            let client = service.device(key.clone()).await?;
            let actual = client.get(None).await?.output;
            let projected = service
                .lookup(key)
                .await?
                .get(key.clone(), None)
                .await?
                .output;
            let progress = client.progress(None).await?.output;
            done &= actual.as_ref().map(DeviceState::snapshot).transpose()?
                == Some(expected.snapshot()?)
                && projected == Some(expected.snapshot()?)
                && progress.is_some_and(|v| v.state == ProjectionState::Delivered);
        }
        let info = service
            .handle
            .queue::<Ingress>()?
            .info(0, None)
            .await?
            .output;
        done &= info.ready == 0 && info.leased == 0 && info.dead == 0;
        if done {
            while let Ok(progress) = events.try_recv() {
                emit(&serde_json::json!({"event":"consumer","progress":progress}))?;
            }
            while let Ok(progress) = summaries.try_recv() {
                emit(&serde_json::json!({"event":"summary","progress":progress}))?;
            }
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(
                "telemetry demo bounded processing/delivery did not converge; preserve active plan"
                    .into(),
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for (key, expected) in plan.devices.iter().zip(&plan.expected) {
        emit(
            &serde_json::json!({"event":"verified_device","device":key,"summary":expected.snapshot()?}),
        )?;
    }
    let Input::Submit { batch } = &plan.requests[2].operation else {
        return Err("invalid demo completion identity".into());
    };
    let completed = state.join(format!("demo-completed-{}.json", batch.id));
    if completed.exists() {
        let existing: Plan = files::load(&completed)?;
        if serde_json::to_vec(&existing)? != serde_json::to_vec(&plan)? {
            return Err("telemetry demo completed plan differs".into());
        }
    } else {
        files::save(&completed, &serde_json::to_vec(&plan)?)?;
    }
    std::fs::remove_file(&active)?;
    std::fs::File::open(state)?.sync_all()?;
    emit(
        &serde_json::json!({"event":"demo_complete","devices":2,"processed_batches":messages,"retained_plan":completed}),
    )
}
