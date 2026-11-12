//! Minimal example for using the rodio-async-sync-bridge crate.

use rodio_async_sync_bridge::{
    AudioSpec,
    MIN_RING_SIZE,
    new_bridge,
};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let samples = [0.0_f32, 0.25, -0.25, 0.0];
    let data: Vec<u8> = samples.iter().flat_map(|sample| sample.to_ne_bytes()).collect();
    let spec = AudioSpec::try_new(48_000, 1).unwrap();

    let (mut producer, consumer) = new_bridge(
        &spec,
        None,
        samples.len(),
        MIN_RING_SIZE,
        tokio::runtime::Handle::current(),
    );

    let producer_task = tokio::spawn(async move {
        producer.write_data(&data).await.expect("consumer closed");
        producer.write_eos().await.expect("consumer closed");
    });

    let received = tokio::task::spawn_blocking(move || consumer.collect::<Vec<_>>())
        .await
        .unwrap();
    producer_task.await.unwrap();
    assert_eq!(received, samples);
}
