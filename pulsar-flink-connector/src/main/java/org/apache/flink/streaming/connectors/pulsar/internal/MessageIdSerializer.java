package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import org.apache.pulsar.client.api.MessageId;

import java.io.IOException;

public class MessageIdSerializer extends TypeSerializer<MessageId> {

    public static final MessageIdSerializer INSTANCE = new MessageIdSerializer();

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<MessageId> duplicate() {
        return this;
    }

    @Override
    public MessageId createInstance() {
        return MessageId.earliest;
    }

    @Override
    public MessageId copy(MessageId from) {
        try {
            return MessageId.fromByteArray(from.toByteArray());
        } catch (IOException e) {
            throw new IllegalStateException("MessageId copy should not throw an exception", e);
        }
    }

    @Override
    public MessageId copy(MessageId from, MessageId reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(MessageId record, DataOutputView target) throws IOException {
        target.write(record.toByteArray());
    }

    @Override
    public MessageId deserialize(DataInputView source) throws IOException {
        int length = source.readInt();
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        return MessageId.fromByteArray(bytes);
    }

    @Override
    public MessageId deserialize(MessageId reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public TypeSerializerSnapshot<MessageId> snapshotConfiguration() {
        return new MessageIdSerializerSnapshot();
    }

    // ------------------------------------------------------------------------

    /**
     * Serializer configuration snapshot for compatibility and format evolution.
     */
    @SuppressWarnings("WeakerAccess")
    public static final class MessageIdSerializerSnapshot extends SimpleTypeSerializerSnapshot<MessageId> {

        public MessageIdSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
