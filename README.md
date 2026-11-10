# Rodio Async Sync Bridge

A `rodio` source that bridges a *async* and a *sync* context.

## Use-cases

The most simplest use-case, and the one this was originally made for, is to have the decoder asynchronously decode and always try to keep the playback buffer full, while also being able to respond to seek requests.

In normal sync rodio, this would be possible, but be very bad for realtime audio, as it could either read a sample from a buffer, or decode more samples, which may also cause file I/O, which can be very slow.

## Implementation

This bridge is implemented via `async-ringbuf` and is currently one-directional (async to sync).

## Usage

Simply insert the source:

```rust
// TODO
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Please see [CONTRIBUTING](CONTRIBUTING.md).
