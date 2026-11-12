//! Demonstrates a mock decoder using the rodio-async-sync-bridge crate.
//!
//! The [`MockDecoder`] mirrors what should be available or implemented on a real audio decoder,
//! but uses a `SineWave` as the audio source instead of decoding real audio data.

use std::{
    error::Error,
    time::Duration,
};

use rodio::{
    Source,
    source::{
        SeekError,
        SineWave,
    },
};
use rodio_async_sync_bridge::{
    AsyncRingConsumer,
    AsyncRingProducer,
    AudioSpec,
    MIN_RING_SIZE,
    new_bridge,
};

const SAMPLE_RATE: u32 = 48_000;
const DURATION: Duration = Duration::from_secs(2);
const BUFFER_SAMPLES: usize = 4096;
const TOTAL_SAMPLES: usize = SAMPLE_RATE as usize * DURATION.as_secs() as usize;

type DynError = Box<dyn Error + Send + Sync>;

#[derive(Clone, Copy)]
struct DecoderSpec {
    rate:     u32,
    channels: u16,
}

/// The Decoder implementation, using a `SineWave` as the audio source.
///
/// This just shows what methods are necessary to implement a decoder compatible with the async ring buffer bridge.
struct MockDecoder {
    source:   SineWave,
    position: usize,
    buffer:   Vec<u8>,
    offset:   usize,
}

impl MockDecoder {
    fn new() -> Self {
        Self {
            source:   SineWave::new(440.0),
            position: 0,
            buffer:   Vec::new(),
            offset:   0,
        }
    }

    fn spec(&self) -> DecoderSpec {
        DecoderSpec {
            rate:     SAMPLE_RATE,
            channels: 1,
        }
    }

    fn duration(&self) -> Option<Duration> {
        Some(DURATION)
    }

    fn buffer_len(&self) -> usize {
        BUFFER_SAMPLES
    }

    fn get_buffer(&self) -> &[u8] {
        &self.buffer[self.offset..]
    }

    fn advance_offset(&mut self, count: usize) {
        self.offset += count;
        assert!(self.offset <= self.buffer.len());
    }

    fn decode(&mut self) -> Option<()> {
        if !self.get_buffer().is_empty() {
            return Some(());
        }

        self.buffer.clear();
        self.offset = 0;
        let count = BUFFER_SAMPLES.min(TOTAL_SAMPLES - self.position);
        if count == 0 {
            return None;
        }

        for sample in self.source.by_ref().take(count) {
            self.buffer.extend_from_slice(&sample.to_ne_bytes());
        }
        self.position += count;
        Some(())
    }

    fn try_seek(&mut self, position: Duration) -> Result<(), SeekError> {
        let position = position.min(DURATION);
        self.source.try_seek(position)?;
        self.position = (position.as_nanos() * u128::from(SAMPLE_RATE) / 1_000_000_000) as usize;
        self.buffer.clear();
        self.offset = 0;
        Ok(())
    }
}

/// The Task that reads the samples from the decoder and writes them to the async ring buffer bridge.
async fn decoder_task(mut decoder: MockDecoder, mut producer: AsyncRingProducer) -> Result<(), DynError> {
    let mut send_eos = false;

    loop {
        if decoder.get_buffer().is_empty() && !send_eos && decoder.decode().is_none() {
            if producer.write_eos().await.is_none() {
                break;
            }
            send_eos = true;
        }

        let seek_future = producer.wait_seek();
        let write_future = producer.write_data(decoder.get_buffer());

        tokio::select! {
            // If there is nothing to write, this future may exit immediately, causing a fast-loop until seek or dropped.
            // that is why the "!send_eos" condition is here: to only listen on the "seek" future.
            written = write_future, if !send_eos => {
                if written.is_none() {
                    break;
                }
                decoder.advance_offset(decoder.get_buffer().len());
            }
            seek = seek_future => {
                let Some(seek) = seek else {
                    break;
                };
                decoder.try_seek(seek.seek_to)?;
                send_eos = false;
                let spec = decoder.spec();
                let spec = AudioSpec::new_checked(spec.rate, spec.channels).unwrap();
                if producer.finish_seek(&spec, decoder.buffer_len(), seek.callback).await.is_none() {
                    break;
                }
            }
        }
    }

    Ok(())
}

/// The Task that reads the samples from the async ring buffer bridge and provides them via a rodio source.
///
/// For this showcase, it just counts the number of samples consumed and seeks once.
fn consumer_task(mut consumer: AsyncRingConsumer) -> Result<usize, DynError> {
    let mut count = 0;
    let mut sought = false;
    while consumer.next().is_some() {
        count += 1;
        if count == 1024 && !sought {
            consumer.try_seek(Duration::ZERO)?;
            sought = true;
        }
    }
    Ok(count)
}

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let decoder = MockDecoder::new();
    let spec = decoder.spec();
    let (producer, consumer) = new_bridge(
        &AudioSpec::new_checked(spec.rate, spec.channels).unwrap(),
        decoder.duration(),
        decoder.buffer_len(),
        MIN_RING_SIZE,
        tokio::runtime::Handle::current(),
    );

    let producer_task = tokio::task::spawn(decoder_task(decoder, producer));

    let consumer_task = tokio::task::spawn_blocking(move || consumer_task(consumer));

    let recieved_samples = consumer_task.await??;
    producer_task.await??;

    // We add samples here, as we have seeked to be beginning once, producing more samples.
    assert_eq!(recieved_samples, TOTAL_SAMPLES + 1024);
    println!("Consumed {recieved_samples} samples");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        BUFFER_SAMPLES,
        DURATION,
        MockDecoder,
    };
    use std::{
        mem::size_of,
        time::Duration,
    };

    #[test]
    fn preserves_buffer_and_resets_on_seek() {
        let mut decoder = MockDecoder::new();
        decoder.decode().unwrap();
        let original = decoder.get_buffer().to_vec();
        assert_eq!(original.len(), BUFFER_SAMPLES * size_of::<rodio::Sample>());

        decoder.advance_offset(size_of::<rodio::Sample>());
        decoder.decode().unwrap();
        assert_eq!(decoder.get_buffer(), &original[size_of::<rodio::Sample>()..]);

        decoder.try_seek(Duration::ZERO).unwrap();
        decoder.decode().unwrap();
        assert_eq!(decoder.get_buffer(), original);

        decoder.try_seek(DURATION + Duration::from_secs(1)).unwrap();
        assert!(decoder.get_buffer().is_empty());
        assert!(decoder.decode().is_none());
    }
}
