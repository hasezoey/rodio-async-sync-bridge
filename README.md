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

```rust
let spec = decoder.spec();

// Note that the following values are just placeholders, and should be adjusted to your needs
let (prod, cons) = new_bridge(
  AudioSpec::new_checked(spec.rate, spec.channels).unwrap(),
  decoder.duration(),
  decoder.buffer_len(),
  MIN_RING_SIZE,
  tokio::runtime::Handle::current()
);

// Append the sync consumer to the rodio sink
sink.append(cons);

let mut send_eos = false;
loop {
  let seek_fut = prod.wait_seek();
  let write_fut = prod.write_data(decoder.get_buffer());

  select! {
    // if there is nothing to write, this future may exit immediately, causing a fast-loop until seek or dropped.
    // that is why condition is here, to only listen on the "seek" future.
    written = write_fut, if !send_eos => {
      decoder.advance_offset(written);
    },
    seek = seek_fut => {
      send_eos = false;
      // "seek" being "None" here means that the seek channel is closed, which in turn means we can abort decoding.
      let seek = seek?;
      
      decoder.try_seek(seek.rate).unwrap();

      let spec = decoder.spec();
      prod.process_seek(&spec.rate, spec.channels, seek.1).await;
    }
  }

  let old_spec = decoder.spec();
  if decoder.decode().is_none() {
    prod.new_eos().await?;
    send_eos = true;
  }
  let new_spec = decoder.spec();
  if old_spec != new_spec {
    prod.new_spec(&new_spec.rate, new_spec.channels);
  }
}
```

## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.

## Contributing

Please see [CONTRIBUTING](./CONTRIBUTING.md).
