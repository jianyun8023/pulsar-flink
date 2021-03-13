package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;

import org.apache.pulsar.client.api.MessageId;

import java.io.IOException;

public class PulsarSourceStateSerializer implements SimpleVersionedSerializer<Tuple2<String, MessageId>> {

    private static final int CURRENT_VERSION = 0;

    private final ExecutionConfig executionConfig;

    public PulsarSourceStateSerializer(ExecutionConfig executionConfig) {
        this.executionConfig = executionConfig;
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(Tuple2<String, MessageId> obj) throws IOException {
//        getSerializer().serialize(obj, );
        return new byte[0];
    }

    @Override
    public Tuple2<String, MessageId> deserialize(int version, byte[] serialized) throws IOException {
        final DataInputDeserializer deserializer = new DataInputDeserializer(serialized);
        return getSerializer().deserialize(deserializer);
    }

    private TupleSerializer<Tuple2<String, MessageId>> getSerializer() {
        TypeSerializer<?>[] fieldSerializers =
                new TypeSerializer<?>[]{
                        StringSerializer.INSTANCE,
                        new KyroS<>(MessageId.class)
                };
        @SuppressWarnings("unchecked")
        Class<Tuple2<String, MessageId>> tupleClass =
                (Class<Tuple2<String, MessageId>>) (Class<?>) Tuple2.class;
        final TupleSerializer<Tuple2<String, MessageId>> serializer =
                new TupleSerializer<>(tupleClass, fieldSerializers);
        return serializer;
    }
}
