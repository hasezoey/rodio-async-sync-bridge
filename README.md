# Rodio Async Sync Bridge

A `rodio` source that bridges a *async* and a *sync* context.

## Use-cases

The most simplest use-case, and the one this was originally made for, is to have the decoder asynchronously decode and always try to keep the playback buffer full, while also being able to respond to seek requests.

In normal sync rodio, this would be possible, but be very bad for realtime audio, as it could either read a sample from a buffer, or decode more samples, which may also cause file I/O, which can be very slow.

## Implementation

This bridge is implemented via `async-ringbuf` and is currently one-directional (async to sync).

Currently, this implementation makes use of `tokio`, but this might be changed in the future to have broader runtime support.

## Usage

The consumer side can be simply plugged in where a source can be put. The producer needs async functions and needs to be called directly:

Runnable examples:

- [Minimal bridge usage](examples/minimal.rs): `cargo run --example minimal`
- [Mock decoding and seeking](examples/mock_decoder.rs): `cargo run --example mock_decoder`

The following are the essential parts from the [Mock decoder](examples/mock_decoder.rs) example:

```rust
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
        let spec = AudioSpec::try_new(spec.rate, spec.channels).unwrap();
        if producer.complete_seek(&spec, decoder.buffer_len(), seek.callback).await.is_none() {
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
fn consumer_task(mut consumer: SyncRingConsumer) -> Result<usize, DynError> {
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
```

## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.

## Contributing

Please see [CONTRIBUTING](./CONTRIBUTING.md).
