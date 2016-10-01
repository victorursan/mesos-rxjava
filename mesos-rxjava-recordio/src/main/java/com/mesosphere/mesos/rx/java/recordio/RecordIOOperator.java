/*
 *    Copyright (C) 2015 Mesosphere, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mesosphere.mesos.rx.java.recordio;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable.Operator;
import rx.Subscriber;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * An {@link Operator} that can be applied to a stream of {@link ByteBuf} and produce
 * a stream of {@code byte[]} messages following the RecordIO format.
 *
 * @see <a href="https://github.com/apache/mesos/blob/master/docs/scheduler-http-api.md#recordio-response-format" target="_blank">RecordIO</a>
 * @see rx.Observable#lift(Operator)
 */
public final class RecordIOOperator implements Operator<byte[], ByteBuf> {

    @Override
    public Subscriber<ByteBuf> call(final Subscriber<? super byte[]> subscriber) {
        return new RecordIOSubscriber(subscriber);
    }

    /**
     * A {@link Subscriber} that can process the contents of a {@link ByteBuf} and emit 0-to-many
     * {@code byte[]} messages. The format of the data stream represented by the {@link ByteBuf}
     * is in
     * <a href="https://github.com/apache/mesos/blob/master/docs/scheduler-http-api.md#recordio-response-format">
     * RecordIO format</a>. If a single {@link ByteBuf} does not represent a full message, the data will be
     * buffered until a full message can be obtained.
     * <p>
     * <i>
     * Due to the way arrays in Java work, there is an effective limitation to message size of
     * 2gb. This is because arrays are indexed with signed 32-bit integers.
     * </i>
     */
    static final class RecordIOSubscriber extends Subscriber<ByteBuf> {
        private static final Logger LOGGER = LoggerFactory.getLogger(RecordIOSubscriber.class);

        @NotNull
        final Subscriber<? super byte[]> child;

        /**
         * The message size from the stream is provided as a base-10 String representation of an
         * unsigned 64 bit integer (uint64). Since there is a possibility (since the spec isn't
         * formal on this, and the HTTP chunked Transfer-Encoding applied to the data stream can
         * allow chunks to be any size) this field functions as the bytes that have been read
         * since the end of the last message. When the next '\n' is encountered in the byte
         * stream, these bytes are turned into a {@code byte[]} and converted to a UTF-8 String.
         * This string representation is then read as a {@code long} using
         * {@link Long#valueOf(String, int)}.
         */
        @NotNull
        final List<Byte> messageSizeBytesBuffer = new ArrayList<>();

        /**
         * The allocated {@code byte[]} for the current message being read from the stream.
         * Once all the bytes of the message have been read this reference will be
         * nulled out until the next message size has been resolved.
         */
        byte[] messageBytes = null;
        /**
         * The number of bytes in the encoding is specified as an unsigned (uint64)
         * However, since arrays in java are addressed and indexed by int we drop the
         * precision early so that working with the arrays is easier.
         * Also, a byte[Integer.MAX_VALUE] is 2gb so I seriously doubt we'll be receiving
         * a message that large.
         */
        int remainingBytesForMessage = 0;

        RecordIOSubscriber(@NotNull final Subscriber<? super byte[]> child) {
            super(child);
            this.child = child;
        }

        private static byte[] getByteArray(@NotNull final List<Byte> list) {
            final byte[] bytes = new byte[list.size()];
            IntStream.range(0, list.size())
                    .forEach(i -> bytes[i] = list.get(i));
            return bytes;
        }

        @Override
        public void onStart() {
            request(Long.MAX_VALUE);
        }

        /**
         * When a {@link ByteBuf} is passed into this method it is completely "drained".
         * Meaning all bytes are read from it and any message(s) contained in it will be
         * extracted and then sent to the child via {@link Subscriber#onNext(Object)}.
         * If any error is encountered (exception) {@link RecordIOSubscriber#onError(Throwable)}
         * will be called and the method will terminate without attempting to do any
         * sort of recovery.
         *
         * @param t The {@link ByteBuf} to process
         */
        @Override
        public void onNext(final ByteBuf t) {
            try (final ByteBufInputStream in = new ByteBufInputStream(t)) {
                while (t.readableBytes() > 0) {
                    // New message
                    if (remainingBytesForMessage == 0) {

                        // Figure out the size of the message
                        byte b;
                        while ((b = (byte) in.read()) != -1 && b != (byte) '\n') {
                            messageSizeBytesBuffer.add(b);
                        }

                        // Allocate the byte[] for the message and get ready to read it
                        if (b == (byte) '\n') {
                            final byte[] bytes = getByteArray(messageSizeBytesBuffer);
                            final String sizeString = new String(bytes, StandardCharsets.UTF_8);
                            messageSizeBytesBuffer.clear();
                            remainingBytesForMessage = getMaxRemaining(sizeString);
                            messageBytes = new byte[remainingBytesForMessage];
                        }
                    }

                    // read bytes until we either reach the end of the ByteBuf or the message is fully read.
                    readFullMessage(t, in);
                }
            } catch (Exception e) {
                onError(e);
            }
        }

        private int getMaxRemaining(@NotNull final String stringValue) {
            final long l = Long.valueOf(stringValue, 10);
            if (l > Integer.MAX_VALUE) {
                LOGGER.warn("specified message size ({}) is larger than Integer.MAX_VALUE. Value will be truncated to int");
                return Integer.MAX_VALUE;
                // TODO: Possibly make this more robust to account for things larger than 2g
            }
            return (int) l;
        }

        private void readFullMessage(@NotNull final ByteBuf byteBuf, @NotNull final ByteBufInputStream in) throws IOException {
            final int readableBytes = byteBuf.readableBytes();
            if (readableBytes > 0) {
                final int writeStart = messageBytes.length - remainingBytesForMessage;
                final int numBytesToCopy = Math.min(readableBytes, remainingBytesForMessage);
                final int read = in.read(messageBytes, writeStart, numBytesToCopy);
                remainingBytesForMessage -= read;
            }

            // Once we've got a full message send it on downstream.
            if (remainingBytesForMessage == 0 && messageBytes != null) {
                child.onNext(messageBytes);
                messageBytes = null;
            }
        }

        @Override
        public void onError(final Throwable e) {
            child.onError(e);
        }

        @Override
        public void onCompleted() {
            child.onCompleted();
        }

    }

}
