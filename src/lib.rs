#![cfg_attr(not(doctest), doc = include_str!("../README.md"))]

use std::{
    fmt::Debug,
    future::Future,
    iter::FusedIterator,
    sync::Arc,
    time::Duration,
};

use async_ringbuf::{
    AsyncHeapRb,
    WaitError,
    traits::{
        AsyncConsumer,
        AsyncProducer,
        Consumer,
        Observer,
        Producer,
        Split,
    },
};
use log::trace;
use parking_lot::RwLock;
use rodio::{
    ChannelCount,
    Sample,
    SampleRate,
    Source,
};
use tokio::{
    runtime::Handle,
    sync::{
        mpsc,
        oneshot,
    },
};

use messages::{
    MessageDataActual,
    MessageDataFirst,
    MessageDataValue,
    MessageSpec,
    MessageSpecResult,
    RingMessages,
    RingMsgParse2,
    RingMsgWrite2,
};
use wrap::{
    ConsWrap,
    ProdWrap,
};

mod messages;
mod wrap;

pub use messages::AudioSpec;

/// The callback for when the seek has been processed and is complete on the decoder.
#[derive(Debug)]
pub struct SeekCallback(oneshot::Sender<usize>);

/// Data to send over the Seek Channel.
#[derive(Debug)]
pub struct SeekData {
    /// The point to seek to in the decoder.
    pub seek_to:  Duration,
    /// The callback for when the seek has been processed and is complete on the decoder.
    pub callback: SeekCallback,
}

impl SeekData {
    /// Create a new `SeekData` instance.
    pub fn new(seek_to: Duration, callback: SeekCallback) -> Self {
        SeekData { seek_to, callback }
    }
}

/// The minimal size a decode-ringbuffer should have.
///
/// Currently the size is based on 192kHz * 1 channel * 1 seconds, or 2 seconds of 48kHz stereo(2 channel) audio.
pub const MIN_RING_SIZE: usize = 192_000 * MessageDataValue::MESSAGE_SIZE;

/// A ringbuffer Producer meant for async audio decoding and playback.
///
/// The implementation of the Producer is meant for async code, awaiting to fully put data on the ringbuffer OR waiting for seek events.
#[derive(Debug)]
pub struct AsyncRingProducer {
    inner:   ProdWrap,
    seek_rx: Arc<RwLock<mpsc::Receiver<SeekData>>>,

    data: Option<MessageDataActual>,
}

impl AsyncRingProducer {
    fn new(wrap: ProdWrap, seek_rx: mpsc::Receiver<SeekData>) -> Self {
        AsyncRingProducer {
            inner:   wrap,
            seek_rx: Arc::new(RwLock::new(seek_rx)),
            data:    None,
        }
    }

    /// Check if the consumer ([`AsyncRingConsumer`]) is still connected and writes are possible
    #[allow(dead_code)] // can't use expect as this function is used in tests
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Write a new spec to the ring buffer.
    ///
    /// Returns [`Some(count)`](Some) if the message is written, with the length, [`None`] if closed.
    /// The `count` returned represents the total number of bytes written for the current message in this call.
    ///
    /// # Cancel Safety
    ///
    /// This future either completely writes the message, or not at all, making it completely cancel safe.
    #[allow(clippy::missing_panics_doc)] // https://github.com/rust-lang/rust-clippy/issues/14534
    pub async fn write_new_spec(&mut self, spec: &AudioSpec, current_span_len: usize) -> Option<usize> {
        let mut msg_buf = [0; RingMsgWrite2::get_msg_size(MessageSpec::MESSAGE_SIZE)];
        // SAFETY: we allocated the exact necessary size, this can never fail
        // #[expect(
        //     clippy::missing_panics_doc,
        //     reason = "static buffer with exact size required created above"
        // )]
        let _ = RingMsgWrite2::try_write_spec(spec, current_span_len, &mut msg_buf).unwrap();

        // Wait for enough free space to write everything at once.
        // With "push_exact", it could write the data partially, which is not cancel safe.
        self.inner.wait_vacant(msg_buf.len()).await.ok()?;
        self.inner.push_slice(&msg_buf);

        Some(msg_buf.len())
    }

    /// Write the header of the data message, without writing any data bytes yet.
    ///
    /// Returns [`Some(count)`](Some) if the message is written, with the length, [`None`] if closed.
    ///
    /// Returns [`None`] if the ring buffer is closed.
    #[allow(clippy::missing_panics_doc)] // https://github.com/rust-lang/rust-clippy/issues/14534
    async fn write_data_header(&mut self, length: usize) -> Option<usize> {
        let mut msg_buf = [0; RingMsgWrite2::get_msg_size(MessageDataFirst::MESSAGE_SIZE)];
        // SAFETY: we allocated the exact necessary size, this can never fail
        // #[expect(
        //     clippy::missing_panics_doc,
        //     reason = "static buffer with exact size required created above"
        // )]
        let (data, _written) = RingMsgWrite2::try_write_data_first(length, &mut msg_buf).unwrap();

        // Wait for enough free space to write everything at once.
        // With "push_exact", it could write the data partially, which is not cancel safe.
        self.inner.wait_vacant(msg_buf.len()).await.ok()?;
        self.inner.push_slice(&msg_buf);

        self.data = Some(data);

        Some(msg_buf.len())
    }

    /// Write a buffer's content.
    ///
    /// This functions expects a data message to be active.
    ///
    /// Returns [`Some(count)`](Some) if the message is written, [`None`] if closed.
    /// The `count` returned represents the total number of bytes written for the current message in this call,
    /// It does *not* represent the bytes consumed from the buffer.
    async fn write_data_bytes(&mut self, data: &[u8]) -> Option<usize> {
        let Some(msg) = &mut self.data else {
            unimplemented!("This should be checked outside of the function");
        };

        let mut buf = &data[msg.get_range()];
        // This needs to be tracked separately, as "buf" gets sliced down during the loop,
        // and msg keeps track of progress across calls.
        let mut total_written = 0;

        // Write the data in chunks so that we are cancel safe.
        // We cannot use "push_exact" here, as it may not fully write the input, and on cancel
        // we would not get a feedback on how much was actually written.
        while !msg.is_done() && !self.inner.is_closed() {
            // "1024" is arbitrarily chosen. It is used to avoid writing 1 sample at a time and then waiting again.
            let wait_for = buf.len().min(1024);
            self.inner.wait_vacant(wait_for).await.ok()?;
            let chunck_size = buf.len().min(self.inner.vacant_len());

            let written = self.inner.push_slice(&buf[..chunck_size]);
            msg.advance_read(written);
            buf = &buf[written..];
            total_written += written;
        }

        if !msg.is_done() && self.inner.is_closed() {
            return None;
        }

        Some(total_written)
    }

    /// Create a Future that waits until the data is completely written.
    ///
    /// Returns [`Some(count)`](Some) if the message is written, [`None`] if closed.
    /// The `count` returned represents the total number of bytes written for the current message in this call,
    /// It does *not* represent the bytes consumed from the buffer.
    ///
    /// If this function returned `Some`, it means the full buffer was consumed.
    ///
    /// # Safety
    ///
    /// This function expects the same exact buffer across calls, until the message is completely written.
    /// Do not change the buffer (slicing / completely different data)!
    ///
    /// # Cancel Safety
    ///
    /// This future is cancel safe, as long as the next call gets the same exact buffer until the message is complete,
    /// as it only writes complete chunks.
    #[allow(clippy::missing_panics_doc)] // https://github.com/rust-lang/rust-clippy/issues/14534
    pub async fn write_data(&mut self, buf: &[u8]) -> Option<usize> {
        if buf.is_empty() {
            return None;
        }

        let mut written = 0;
        if self.data.is_none() {
            written += self.write_data_header(buf.len()).await?;
        }

        written += self.write_data_bytes(buf).await?;

        self.data.take();

        Some(written)
    }

    /// Write a End-of-Stream (EOS) message.
    ///
    /// Returns [`Some(count)`](Some) if the message is written, [`None`] if closed.
    /// The `count` returned represents the total number of bytes written for the current message in this call.
    ///
    /// # Cancel Safety
    ///
    /// This future either completely writes the message, or not at all, making it completely cancel safe.
    #[allow(clippy::missing_panics_doc)] // https://github.com/rust-lang/rust-clippy/issues/14534
    pub async fn write_eos(&mut self) -> Option<usize> {
        let mut msg_buf = [0; RingMsgWrite2::get_msg_size(0)];
        // SAFETY: we allocated the exact necessary size, this can never fail
        // #[expect(
        //     clippy::missing_panics_doc,
        //     reason = "static buffer with exact size required created above"
        // )]
        let _ = RingMsgWrite2::try_write_eos(&mut msg_buf).unwrap();

        // Wait for enough free space to write everything at once.
        // With "push_exact", it could write the data partially, which is not cancel safe.
        self.inner.wait_vacant(msg_buf.len()).await.ok()?;
        self.inner.push_slice(&msg_buf);

        Some(msg_buf.len())
    }

    /// Create a Future to await until the seek channel is dropped([`None`]) or a seek is requested([`Some`]).
    pub fn wait_seek(&mut self) -> WaitSeek {
        WaitSeek(self.seek_rx.clone())
    }

    /// Process a seek and call the Consumer to resume.
    ///
    /// This clear all data state, calls the consumer to resume with how many bytes to skip
    /// and sends the new spec (in case it changed in the seek).
    ///
    /// Returns [`Some(count)`](Some) if the message is written, [`None`] if closed.
    /// The `count` returned represents the total number of bytes written for the current message in this call.
    ///
    /// # Cancel Safety
    ///
    /// This future is *not* cancel safe. It expects to be awaited to completion once called.
    pub async fn finish_seek(&mut self, spec: &AudioSpec, current_span_len: usize, cb: SeekCallback) -> Option<usize> {
        self.data.take();
        let bytes_to_skip = self.inner.occupied_len();
        let _ = cb.0.send(bytes_to_skip);

        self.write_new_spec(spec, current_span_len).await
    }
}

/// Struct to wait on a channel, without the compiler complaining that `self` can't be borrowed mutably multiple times.
#[derive(Debug)]
pub struct WaitSeek(Arc<RwLock<mpsc::Receiver<SeekData>>>);

impl Future for WaitSeek {
    type Output = Option<SeekData>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        self.0.write().poll_recv(cx)
    }
}

/// Create a new ringbuffer bridge with an async producer and consumer.
///
/// Note that the ringbuffer size will be at least [`MIN_RING_SIZE`].
#[must_use]
pub fn new_bridge(
    spec: &AudioSpec,
    total_duration: Option<Duration>,
    current_span_len: usize,
    size: usize,
    handle: Handle,
) -> (AsyncRingProducer, AsyncRingConsumer) {
    let size = size.max(MIN_RING_SIZE);
    let ringbuf = AsyncHeapRb::<u8>::new(size);
    let (prod, cons) = ringbuf.split();
    let (tx, rx) = mpsc::channel(1);
    let async_prod = AsyncRingProducer::new(ProdWrap::new(prod), rx);
    let async_cons = AsyncRingConsumer::new(
        ConsWrap::new(cons),
        Some(tx),
        spec,
        total_duration,
        current_span_len,
        handle,
    );
    (async_prod, async_cons)
}

/// A sync ringbuffer Consumer
///
/// The implementation of the Consumer is meant for sync code, only blocking when having to wait for more data.
#[derive(Debug)]
pub struct AsyncRingConsumer {
    inner:   ConsWrap,
    /// Send a seek-to value to the producer.
    /// Also used to indicate to the producer that it should keep active.
    seek_tx: Option<mpsc::Sender<SeekData>>,

    // random default size, 1024*32
    buf:      StaticBuf<32768>,
    last_msg: Option<MessageDataActual>,
    handle:   Handle,

    // cached information on how to treat current data until a update
    spec:             AudioSpec,
    /// Always above 0, unless EOS had been reached.
    current_span_len: usize,
    total_duration:   Option<Duration>,
}

impl AsyncRingConsumer {
    /// Create a new consumer for the ringbuffer.
    fn new(
        inner: ConsWrap,
        seek_tx: Option<mpsc::Sender<SeekData>>,
        spec: &AudioSpec,
        total_duration: Option<Duration>,
        current_span_len: usize,
        handle: Handle,
    ) -> Self {
        Self {
            inner,
            seek_tx,
            spec: *spec,
            total_duration,
            current_span_len,
            last_msg: None,
            buf: StaticBuf::new(),
            handle,
        }
    }

    /// Ensure there is a complete message in `last_msg`.
    ///
    /// This function assumes there is no current message.
    #[must_use]
    fn read_msg(&mut self) -> Option<RingMsgParse2> {
        // trace!("Reading a message from the ringbuffer");

        self.load_more_data(1)?;

        assert!(!self.buf.is_empty());

        let detected_type = {
            let detect_byte = {
                // SAFETY: we loaded and asserted that there is at least one byte
                let byte = self.buf.get_ref()[0];
                self.buf.advance_beginning(1);
                byte
            };

            RingMessages::from_u8(detect_byte)
        };

        // Eos event does not have more than the id itself
        if detected_type == RingMessages::Eos {
            return Some(RingMsgParse2::Eos);
        }

        let mut wait_for_bytes = 1;
        let mut total = 0;
        loop {
            total += 1;
            // "buf.is_empty" is safe here as all messages consume the buffer fully here.
            if self.inner.is_closed() && self.inner.is_empty() && self.buf.is_empty() {
                return None;
            }

            self.load_more_data(wait_for_bytes)?;

            // Sanity check against infinite loop
            assert!(total < 10);

            let (msg, read) = match detected_type {
                RingMessages::Data => {
                    let (data_res, read) = match MessageDataFirst::try_read_buf(self.buf.get_ref()) {
                        Ok(v) => v,
                        Err(wait_for) => {
                            wait_for_bytes = wait_for + self.buf.len();
                            continue;
                        },
                    };

                    (RingMsgParse2::Data(data_res), read)
                },
                RingMessages::Spec => {
                    let (spec_res, read) = match MessageSpec::try_read_buf(self.buf.get_ref()) {
                        Ok(v) => v,
                        Err(wait_for) => {
                            wait_for_bytes = wait_for + self.buf.len();
                            continue;
                        },
                    };

                    self.apply_spec_msg(spec_res);

                    (RingMsgParse2::Spec, read)
                },
                RingMessages::Eos => unreachable!("Message EOS is returned earlier"),
            };

            assert!(read > 0);

            self.buf.advance_beginning(read);

            return Some(msg);
        }
    }

    /// Loads more data into the current buffer, if the current buffer does not have at least `wait_for_bytes` bytes.
    ///
    /// This function will **not** block when there is `wait_for_bytes` available, but **will** block otherwise.
    ///
    /// Returns [`Some`] if the current buffer now has at least `wait_for_bytes` buffered, [`None`] if the buffer closed and not enough can be loaded anymore.
    fn load_more_data(&mut self, wait_for_bytes: usize) -> Option<()> {
        if self.buf.len() >= wait_for_bytes {
            return Some(());
        }

        self.buf.maybe_need_move();

        // wait for at least one element being occupied,
        // more elements would mean to wait until they all are there, regardless if they are part of the message or not

        // Avoid calling into async-runtime and async code if the ringbuffer knowingly already contains enough bytes
        // as reading can be done sync, non-blocking for buffer copies.
        // Also SAFETY: "occupied_len" says it "could be more or less" but we are in the consumer here, so we known it won't *decrease*
        // between here and actually reading it.
        if self.inner.occupied_len() < wait_for_bytes {
            // Avoid having to call async stuff for as long as possible, as that can heavily increase CPU load in a hot path.
            // When not doing this, cpu load can be 1.0~1.4 on average.
            // When doing the current way, the load is ~0.5~0.6 on average, the same as if running the decoder directly as
            // as source instead of using this ringbuffer.
            if let Err(err) = self.handle.block_on(self.inner.wait_occupied(wait_for_bytes))
                && err != WaitError::Closed
            {
                panic!("Failed to wait for occupied bytes: {:?}", err);
            }
        }

        if self.inner.is_closed() && self.inner.is_empty() {
            return None;
        }

        let written = self.inner.pop_slice(self.buf.get_spare_mut());
        self.buf.advance_len(written);

        // Sanity, this point should never be reached if the buffer contains enough data or there is no more data to read
        debug_assert!(written > 0);

        Some(())
    }

    /// Apply a new spec from the current message.
    ///
    /// This function assumes the current message is a [`MessageSpec`].
    fn apply_spec_msg(&mut self, new_spec: MessageSpecResult) {
        self.spec = new_spec.spec;
    }

    /// Read data from a Data Message.
    ///
    /// This function assumes the current message is a non-finished [`MessageDataActual`].
    #[must_use]
    fn read_data(&mut self) -> Option<Sample> {
        // trace!("Reading Data");

        // wait until we have enough data to parse a value
        self.load_more_data(MessageDataValue::MESSAGE_SIZE)?;

        assert!(self.buf.len() >= MessageDataValue::MESSAGE_SIZE);

        let msg = self.last_msg.as_mut().unwrap();

        // unwrap: should never panic as we explicitly load at least the required amount above.
        #[allow(clippy::missing_panics_doc)]
        let (sample, read) = msg.try_read_buf(self.buf.get_ref()).unwrap();
        self.buf.advance_beginning(read);

        if msg.is_done() {
            self.last_msg.take();
        }

        Some(sample)
    }

    /// Get whether the Seek channel is still open and available.
    fn is_seek_channel_active(&self) -> bool {
        self.seek_tx.as_ref().is_some_and(|v| !v.is_closed())
    }
}

impl Source for AsyncRingConsumer {
    #[inline]
    fn current_span_len(&self) -> Option<usize> {
        // this workaround might not be great, but on "None" rodio *never* retries reading this value
        // but if we return "0", it assumes the stream is done.
        if self.current_span_len == 0 && self.is_seek_channel_active() {
            return Some(1);
        }

        Some(self.current_span_len)
    }

    #[inline]
    #[allow(clippy::cast_possible_truncation)]
    fn channels(&self) -> ChannelCount {
        self.spec.channels()
    }

    #[inline]
    fn sample_rate(&self) -> SampleRate {
        self.spec.rate()
    }

    #[inline]
    fn total_duration(&self) -> Option<Duration> {
        self.total_duration
    }

    #[inline]
    fn try_seek(&mut self, pos: Duration) -> Result<(), rodio::source::SeekError> {
        trace!("Consumer Seek to position {pos:#?}");

        // clear the ringbuffer before sending, in case it is full and to more quickly unblock the producer
        // though this should not be relied upon
        self.inner.clear();
        self.last_msg.take();
        self.buf.clear();

        let (cb_tx, cb_rx) = oneshot::channel();
        let _ = self
            .seek_tx
            .as_mut()
            .unwrap()
            .blocking_send(SeekData::new(pos, SeekCallback(cb_tx)));

        // Wait for the Producer to have processed the seek and get the final value of elements to skip
        let to_skip = cb_rx.blocking_recv().map_err(|_| {
            rodio::source::SeekError::Other(Arc::from(std::io::Error::other(
                "Seek Callback channel exited unexpectedly",
            )))
        })?;

        // skip possible new elements
        let _ = self.inner.skip(to_skip);
        trace!("Consumer Seek Done");

        Ok(())
    }
}

impl Iterator for AsyncRingConsumer {
    type Item = Sample;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        // This uses the seek channel as a indicator to see if the decoder is still active.
        // This initial "0" may happen in symphonia for whatever reason in ogg-vorbis, see https://github.com/tramhao/termusic/issues/566
        // The seek channel can be used this way, as it only gets closed by the consumer (see the EOS path in the match below)
        // or if the decoder has exited (via error or something).
        if self.current_span_len == 0 && !self.is_seek_channel_active() {
            return None;
        }

        loop {
            if self.last_msg.is_some() {
                let sample = self.read_data();

                return sample;
            }

            if self.inner.is_empty() && self.inner.is_closed() {
                trace!("Ring buffer is empty and closed");
                return None;
            }

            let msg = self.read_msg()?;

            match msg {
                RingMsgParse2::Spec => {},
                RingMsgParse2::Data(message_data_actual) => {
                    self.last_msg = Some(message_data_actual);
                },
                RingMsgParse2::Eos => {
                    trace!("Reached EOS message");
                    let _ = self.seek_tx.take();
                    // this indicates to rodio via Source::current_span_len that there is no more data
                    // and we also use it to uphold the contract with FusedIterator.
                    self.current_span_len = 0;
                    return None;
                },
            }
        }
    }
}

// Contract: once reaching a EOS or the ringbuffer being closed & having read all data, it will continue outputting None
impl FusedIterator for AsyncRingConsumer {}

/// Static Buffer allocated on the stack, which can have a moving area of actual data within
#[derive(Debug, Clone, Copy)]
struct StaticBuf<const N: usize> {
    buf:            [u8; N],
    /// The length of the good data. 0 means 0 good elements (like `.len()`)
    used_len:       usize,
    /// The length of data to skip until next clear / [`make_beginning`](Self::make_beginning) call.
    data_start_idx: usize,
}

impl<const N: usize> StaticBuf<N> {
    const CAPACITY: usize = N;

    /// Create a new buffer.
    ///
    /// Size must be above 0 and and divideable by 2.
    fn new() -> Self {
        const {
            assert!(N > 0);
            assert!(N.is_multiple_of(2));
        }
        Self {
            buf:            [0; N],
            used_len:       0,
            data_start_idx: 0,
        }
    }

    /// The length of actual data in the buffer
    #[inline]
    fn len(&self) -> usize {
        self.get_ref().len()
    }

    /// Returns `true` if there is currently no good data in the buffer
    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Rest good data length.
    #[inline]
    fn clear(&mut self) {
        self.data_start_idx = 0;
        self.used_len = 0;

        // // DEBUG: this should not be necessary, but for debugging the buffer
        // self.buf.fill(u8::MAX);
    }

    /// Get a mutable reference to the slice from `data_start` until end.
    ///
    /// May contain bad data.
    /// And [`advance_len`](Self::advance_len) needs to be called afterward with the written size.
    #[inline]
    #[allow(unused)]
    fn get_mut(&mut self) -> &mut [u8] {
        &mut self.buf[self.data_start_idx..]
    }

    /// Get a mutable reference to the unused portion of the buffer (starting from `len`)
    ///
    /// May contain bad data.
    /// And [`advance_len`](Self::advance_len) needs to be called afterward with the written size.
    #[inline]
    fn get_spare_mut(&mut self) -> &mut [u8] {
        &mut self.buf[self.used_len..]
    }

    /// Get a reference to the slice which contains good data
    #[inline]
    fn get_ref(&self) -> &[u8] {
        &self.buf[self.data_start_idx..self.used_len]
    }

    /// Move the data to the beginning, if start is above half the capacity
    #[inline]
    fn maybe_need_move(&mut self) {
        // Fast-path: clear if start idx is above 0 and there are no good elements
        if self.data_start_idx > 0 && self.is_empty() {
            self.clear();
            return;
        }

        // Only move to the beginning if the start idx is above half the buffer size.
        if self.data_start_idx > Self::CAPACITY / 2 {
            self.make_beginning();
        }
    }

    /// Move the data to beginning if it is not already.
    fn make_beginning(&mut self) {
        let range = self.data_start_idx..self.used_len;
        let range_len = range.len();
        self.buf.copy_within(range, 0);
        self.used_len = range_len;
        self.data_start_idx = 0;

        // // DEBUG: this should not be necessary, but for debugging the buffer
        // self.buf[range_len..].fill(u8::MAX);
    }

    /// Advance the initialized data length by the given count.
    ///
    /// # Panics
    ///
    /// If the given `by` count plus the current `length` will result in higher than `capacity`.
    #[inline]
    fn advance_len(&mut self, by: usize) {
        self.set_len(self.len() + by);
    }

    /// Set the length of the buffer to the written size plus data start, ie how the buffer was given from [`get_mut`](Self::get_mut).
    ///
    /// # Panics
    ///
    /// If the given `written` plus the current `start_idx` will result in higher than `capacity`.
    #[inline]
    fn set_len(&mut self, written: usize) {
        self.used_len = self.data_start_idx + written;
        assert!(self.used_len <= self.buf.len());
    }

    /// Advance the start index.
    ///
    /// Use [`make_beginning`](Self::make_beginning) to move all data to the front again.
    ///
    /// # Panics
    ///
    /// If the given `by` count plus the current `start_idx` will result in higher than `capacity`.
    #[inline]
    fn advance_beginning(&mut self, by: usize) {
        self.data_start_idx += by;
        assert!(self.data_start_idx <= self.buf.len());

        // // DEBUG: this should not be necessary, but for debugging the buffer
        // self.buf[0..self.data_start_idx].fill(u8::MAX);
    }
}

#[cfg(test)]
#[allow(clippy::assertions_on_constants)] // make sure that tests fail if expectations change
mod tests {

    mod static_buffer {
        use crate::StaticBuf;

        #[test]
        fn should_work() {
            let mut buf = StaticBuf::<32>::new();
            assert_eq!(buf.len(), 0);
            assert_eq!(buf.get_ref().len(), 0);
            assert_eq!(buf.len(), buf.used_len);
            assert_eq!(buf.get_mut().len(), 32);
            assert_eq!(buf.get_ref(), &[0u8; 0]);

            buf.get_mut()[0] = u8::MAX;
            buf.set_len(1);

            assert_eq!(buf.len(), 1);
            assert_eq!(buf.get_ref().len(), 1);
            assert_eq!(buf.len(), buf.used_len);
            assert_eq!(buf.get_mut().len(), 32);
            assert_eq!(buf.get_ref(), &[u8::MAX; 1]);

            buf.advance_beginning(1);
            assert_eq!(buf.len(), 0);
            assert_eq!(buf.used_len, 1);
            assert_ne!(buf.len(), buf.used_len);
            assert_eq!(buf.get_mut().len(), 31);
            assert_eq!(buf.get_ref(), &[0u8; 0]);

            buf.get_mut().fill(u8::MAX);
            buf.set_len(31);

            assert_eq!(buf.len(), 31);
            assert_eq!(buf.used_len, 32);
            assert_ne!(buf.len(), buf.used_len);
            assert_eq!(buf.get_mut().len(), 31);
            assert_eq!(buf.get_ref(), &[u8::MAX; 31]);

            buf.make_beginning();

            assert_eq!(buf.len(), 31);
            assert_eq!(buf.len(), buf.used_len);
            assert_eq!(buf.get_mut().len(), 32);
            assert_eq!(buf.get_ref(), &[u8::MAX; 31]);

            buf.advance_beginning(15);

            assert_eq!(buf.len(), 16);
            assert_ne!(buf.len(), buf.used_len);
            assert_eq!(buf.used_len, 31);
            assert_eq!(buf.get_mut().len(), 17);
            assert_eq!(buf.get_ref(), &[u8::MAX; 16]);

            buf.clear();

            assert_eq!(buf.len(), 0);
            assert_eq!(buf.len(), buf.used_len);
            assert_eq!(buf.get_ref().len(), 0);
            assert_eq!(buf.get_mut().len(), 32);
            assert_eq!(buf.get_ref(), &[0u8; 0]);
        }

        #[test]
        #[should_panic(expected = "self.used_len <= self.buf.len()")]
        fn length_capacity() {
            let mut buf = StaticBuf::<16>::new();
            buf.set_len(17);
        }

        #[test]
        #[should_panic(expected = "self.data_start_idx <= self.buf.len()")]
        fn beginning_capacity() {
            let mut buf = StaticBuf::<16>::new();
            buf.advance_beginning(17);
        }

        #[test]
        fn advance_length() {
            let mut buf = StaticBuf::<32>::new();
            assert_eq!(buf.len(), 0);
            assert_eq!(buf.get_ref().len(), 0);
            assert_eq!(buf.get_mut().len(), 32);
            assert_eq!(buf.get_spare_mut().len(), 32);
            assert_eq!(buf.get_ref(), &[0u8; 0]);

            buf.get_mut()[..4].fill(4);
            buf.advance_len(4);

            assert_eq!(buf.len(), 4);
            assert_eq!(buf.get_ref().len(), 4);
            assert_eq!(buf.get_mut().len(), 32);
            assert_eq!(buf.get_spare_mut().len(), 28);
            let expected = &[4u8; 4];
            assert_eq!(buf.get_ref(), expected);

            buf.get_spare_mut()[..4].fill(6);
            buf.advance_len(4);

            assert_eq!(buf.len(), 8);
            assert_eq!(buf.get_ref().len(), 8);
            assert_eq!(buf.get_mut().len(), 32);
            assert_eq!(buf.get_spare_mut().len(), 24);
            let expected: Vec<_> = [4u8; 4].into_iter().chain([6u8; 4]).collect();
            assert_eq!(buf.get_ref(), &expected);

            buf.advance_beginning(5);

            assert_eq!(buf.len(), 3);
            assert_eq!(buf.get_ref().len(), 3);
            assert_eq!(buf.get_mut().len(), 27);
            assert_eq!(buf.get_spare_mut().len(), 24);
            let expected = &[6u8; 3];
            assert_eq!(buf.get_ref(), expected);
        }
    }

    mod ringbuffer {
        use std::{
            future::Future,
            sync::Arc,
            task::Poll,
            time::Duration,
        };

        use async_ringbuf::traits::Observer;
        use parking_lot::Mutex;
        use rodio::Sample;

        use crate::{
            AudioSpec,
            MIN_RING_SIZE,
            MessageDataFirst,
            MessageSpec,
            RingMsgWrite2,
            new_bridge,
        };

        #[tokio::test]
        async fn should_work() {
            let mut send = Vec::new();
            let recv = Arc::new(Mutex::new(Vec::new()));

            let (mut prod, mut cons) = new_bridge(
                &AudioSpec::new_checked(44000, 2).unwrap(),
                None,
                1024,
                0,
                tokio::runtime::Handle::current(),
            );

            assert_eq!(prod.inner.capacity().get(), MIN_RING_SIZE);

            let recv_c = recv.clone();
            let handle = tokio::task::spawn_blocking(move || {
                let mut lock = recv_c.lock();
                for num in cons.by_ref() {
                    lock.extend_from_slice(&num.to_ne_bytes());
                }
                assert_eq!(cons.inner.occupied_len(), 0);
            });

            let first_data = 1f32.to_le_bytes().repeat(1024);
            let written = prod.write_data(&first_data).await.unwrap();
            assert_eq!(
                written,
                RingMsgWrite2::get_msg_size(MessageDataFirst::MESSAGE_SIZE + first_data.len())
            );
            send.extend_from_slice(&first_data);

            let new_spec = AudioSpec::new_checked(48000, 2).unwrap();
            let written = prod.write_new_spec(&new_spec, 1024).await.unwrap();
            assert_eq!(written, RingMsgWrite2::get_msg_size(MessageSpec::MESSAGE_SIZE));

            let second_data = 2f32.to_le_bytes().repeat(1024);
            let written = prod.write_data(&second_data).await.unwrap();
            assert_eq!(
                written,
                RingMsgWrite2::get_msg_size(MessageDataFirst::MESSAGE_SIZE + second_data.len())
            );
            send.extend_from_slice(&second_data);

            let written = prod.write_eos().await.unwrap();
            assert_eq!(written, RingMsgWrite2::get_msg_size(0));

            assert!(prod.write_data(&[]).await.is_none());

            // just to prevent an infinitely running test due to a deadlock
            let res = tokio::time::timeout(Duration::from_secs(3), handle).await.is_ok();
            assert!(res, "Read Task did not complete within 3 seconds");

            assert!(prod.is_closed());
            assert!(prod.inner.is_empty());

            let recv_lock = recv.lock();
            let value_size = size_of::<Sample>();
            assert_eq!(send.len(), value_size * 1024 * 2);
            assert_eq!(recv_lock.len(), value_size * 1024 * 2);

            assert_eq!(*send, *recv_lock);
        }

        #[tokio::test]
        async fn resumes_cancelled_data_write() {
            let (mut prod, cons) = new_bridge(
                &AudioSpec::new_checked(44000, 2).unwrap(),
                None,
                1024,
                0,
                tokio::runtime::Handle::current(),
            );

            // Make the message larger than the ring so a write cannot finish while the consumer is idle.
            let samples: Vec<Sample> = (0..MIN_RING_SIZE / size_of::<Sample>() + 1024)
                .map(|index| (index % 97) as Sample / 97.0)
                .collect();
            let data: Vec<u8> = samples.iter().flat_map(|sample| sample.to_ne_bytes()).collect();

            {
                let future = prod.write_data(&data);
                tokio::pin!(future);
                // One poll writes the header and as much payload as fits, then waits for more space.
                // Returning Ready on Pending lets this scope drop the still-pending write future.
                std::future::poll_fn(|context| match future.as_mut().poll(context) {
                    Poll::Pending => Poll::Ready(()),
                    Poll::Ready(_) => panic!("write completed before cancellation"),
                })
                .await;
            }

            // Keep the consumer idle until after cancellation, ensuring the first poll filled the ring.
            assert_eq!(prod.inner.vacant_len(), 0);
            let message = prod.data.as_ref().unwrap();
            assert!(message.get_range().start > 0);
            assert!(!message.is_done());

            // Retry with the same input; the consumer checks that the already-written prefix is not duplicated.
            let handle = tokio::task::spawn_blocking(move || cons.collect::<Vec<_>>());
            let result = tokio::time::timeout(Duration::from_secs(3), async {
                let written = prod.write_data(&data).await.unwrap();
                assert!(written < data.len());
                prod.write_eos().await.unwrap();
            })
            .await;
            drop(prod);

            assert!(result.is_ok(), "Write did not complete within 3 seconds");
            let received = tokio::time::timeout(Duration::from_secs(3), handle)
                .await
                .expect("Read task did not complete within 3 seconds")
                .unwrap();
            assert_eq!(received, samples);
        }

        // the producer should not exit before the consumer in a actual use-case
        // as the producer may need to still process and output a seek request
        #[tokio::test]
        async fn prod_should_not_exist_before_cons() {
            let order = Arc::new(Mutex::new(Vec::new()));

            let (mut prod, mut cons) = new_bridge(
                &AudioSpec::new_checked(44000, 2).unwrap(),
                None,
                1024,
                0,
                tokio::runtime::Handle::current(),
            );

            let obsv = prod.inner.observe();

            assert!(obsv.read_is_held());
            assert!(obsv.write_is_held());
            let order_c = order.clone();

            let cons_handle = tokio::task::spawn_blocking(move || {
                for num in cons.by_ref() {
                    let _ = num;
                }
                order_c.lock().push("recv_eos");
                assert_eq!(cons.inner.occupied_len(), 0);
                assert!(cons.seek_tx.is_none());
            });

            let obsv_c = obsv.clone();
            let order_c = order.clone();

            let prod_handle = tokio::task::spawn(async move {
                let first_data = 1f32.to_le_bytes().repeat(1024);
                let written = prod.write_data(&first_data).await.unwrap();
                assert_eq!(
                    written,
                    RingMsgWrite2::get_msg_size(MessageDataFirst::MESSAGE_SIZE + first_data.len())
                );

                assert!(obsv_c.read_is_held());
                assert!(obsv_c.write_is_held());

                order_c.lock().push("send_eos");
                let written = prod.write_eos().await.unwrap();
                assert_eq!(written, RingMsgWrite2::get_msg_size(0));

                // "wait_seek" can only return here if the channel gets closed, which happens when the consumer reaches the EOS message (not just on drop)
                let _ = prod.wait_seek().await;
                assert!(prod.seek_rx.read().is_closed());
                order_c.lock().push("prod");
            });

            // just to prevent an infinitely running test due to a deadlock
            let res = tokio::time::timeout(Duration::from_secs(3), cons_handle).await.is_ok();
            assert!(res, "Read Task did not complete within 3 seconds");

            assert!(!obsv.read_is_held());

            // just to prevent an infinitely running test due to a deadlock
            let res = tokio::time::timeout(Duration::from_secs(3), prod_handle).await.is_ok();
            assert!(res, "Write Task did not complete within 3 seconds");

            assert!(!obsv.write_is_held());

            // consumer should always exit first (unless explicit tests), because the consumer may signal a seek where the producer needs to respond to, even if a EOS was already send.
            let lock = order.lock();
            // "send_eos" is always ensured to happen before the other events
            assert_eq!(&lock[..1], &["send_eos"]);
            // but any other depend on the scheduling, unless the actual code is modified to send those events (which is not done)
            assert!(lock[1..].contains(&"recv_eos"));
            assert!(lock[1..].contains(&"prod"));
            assert_eq!(lock.len(), 3);
        }

        // even if the producer (due to some error or otherwise) exits with eos, the consumer should consume everything still available
        #[tokio::test]
        async fn should_consume_on_prod_exit_eos() {
            let recv = Arc::new(Mutex::new(Vec::new()));

            let (mut prod, mut cons) = new_bridge(
                &AudioSpec::new_checked(44000, 2).unwrap(),
                None,
                1024,
                0,
                tokio::runtime::Handle::current(),
            );

            let recv_c = recv.clone();
            let handle = tokio::task::spawn_blocking(move || {
                let mut lock = recv_c.lock();
                for num in cons.by_ref() {
                    lock.extend_from_slice(&num.to_ne_bytes());
                }
                assert_eq!(cons.inner.occupied_len(), 0);
                assert!(!cons.inner.write_is_held());
            });

            let first_data = 1f32.to_le_bytes().repeat(1024);
            let written = prod.write_data(&first_data).await.unwrap();
            assert_eq!(
                written,
                RingMsgWrite2::get_msg_size(MessageDataFirst::MESSAGE_SIZE + first_data.len())
            );

            let written = prod.write_eos().await.unwrap();
            assert_eq!(written, RingMsgWrite2::get_msg_size(0));

            let obsv = prod.inner.observe();
            drop(prod);

            assert!(!obsv.write_is_held());
            // dont check read as that *could* have consumed and exited already
            // assert_eq!(obsv.read_is_held(), true);

            // just to prevent an infinitely running test due to a deadlock
            let res = tokio::time::timeout(Duration::from_secs(3), handle).await.is_ok();
            assert!(res, "Read Task did not complete within 3 seconds");

            assert!(!obsv.write_is_held());
            assert!(!obsv.read_is_held());

            let recv_lock = recv.lock();
            let value_size = size_of::<Sample>();
            assert_eq!(recv_lock.len(), value_size * 1024);

            assert_eq!(*recv_lock, first_data.as_slice());
        }

        // even if the producer (due to some error or otherwise) exits without, the consumer should consume everything still available
        #[tokio::test]
        async fn should_consume_on_prod_exit() {
            let recv = Arc::new(Mutex::new(Vec::new()));

            let (mut prod, mut cons) = new_bridge(
                &AudioSpec::new_checked(44000, 2).unwrap(),
                None,
                1024,
                0,
                tokio::runtime::Handle::current(),
            );

            let recv_c = recv.clone();
            let handle = tokio::task::spawn_blocking(move || {
                let mut lock = recv_c.lock();
                for num in cons.by_ref() {
                    lock.extend_from_slice(&num.to_ne_bytes());
                }
                assert_eq!(cons.inner.occupied_len(), 0);
                assert!(!cons.inner.write_is_held());
            });

            let first_data = 1f32.to_le_bytes().repeat(1024);
            let written = prod.write_data(&first_data).await.unwrap();
            assert_eq!(
                written,
                RingMsgWrite2::get_msg_size(MessageDataFirst::MESSAGE_SIZE + first_data.len())
            );

            let obsv = prod.inner.observe();
            drop(prod);

            assert!(!obsv.write_is_held());
            // dont check read as that *could* have consumed and exited already
            // assert_eq!(obsv.read_is_held(), true);

            // just to prevent an infinitely running test due to a deadlock
            let res = tokio::time::timeout(Duration::from_secs(3), handle).await.is_ok();
            assert!(res, "Read Task did not complete within 3 seconds");

            assert!(!obsv.write_is_held());
            assert!(!obsv.read_is_held());

            let recv_lock = recv.lock();
            let value_size = size_of::<Sample>();
            assert_eq!(recv_lock.len(), value_size * 1024);

            assert_eq!(*recv_lock, first_data.as_slice());
        }
    }
}
