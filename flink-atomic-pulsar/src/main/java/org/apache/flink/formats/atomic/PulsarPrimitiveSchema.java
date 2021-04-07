/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.atomic;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.pulsar.client.api.Schema;

import java.io.IOException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.shaded.guava18.com.google.common.base.Preconditions.checkArgument;

/**
 * pulsar primitive deserialization.
 */
public class PulsarPrimitiveSchema<T> implements SerializationSchema<T>, DeserializationSchema<T> {

    private static final Map<Class<?>, Schema<?>> pulsarPrimitives = new HashMap<>();

    static {
        pulsarPrimitives.put(Boolean.class, Schema.BOOL);
        pulsarPrimitives.put(Boolean.TYPE, Schema.BOOL);
        pulsarPrimitives.put(Byte.class, Schema.INT8);
        pulsarPrimitives.put(Byte.TYPE, Schema.INT8);
        pulsarPrimitives.put(Short.class, Schema.INT16);
        pulsarPrimitives.put(Short.TYPE, Schema.INT16);
        pulsarPrimitives.put(Integer.class, Schema.INT32);
        pulsarPrimitives.put(Integer.TYPE, Schema.INT32);
        pulsarPrimitives.put(Long.class, Schema.INT64);
        pulsarPrimitives.put(Long.TYPE, Schema.INT64);
        pulsarPrimitives.put(String.class, Schema.STRING);
        pulsarPrimitives.put(Float.class, Schema.FLOAT);
        pulsarPrimitives.put(Float.TYPE, Schema.FLOAT);
        pulsarPrimitives.put(Double.class, Schema.DOUBLE);
        pulsarPrimitives.put(Double.TYPE, Schema.DOUBLE);
        pulsarPrimitives.put(Byte[].class, Schema.BYTES);
        pulsarPrimitives.put(Date.class, Schema.DATE);
        pulsarPrimitives.put(Time.class, Schema.TIME);
        pulsarPrimitives.put(Timestamp.class, Schema.TIMESTAMP);
//        pulsarPrimitives.put(LocalDate.class, Schema.LOCAL_DATE);
//        pulsarPrimitives.put(LocalTime.class, Schema.LOCAL_TIME);
//        pulsarPrimitives.put(LocalDateTime.class, Schema.LOCAL_DATE_TIME);
//        pulsarPrimitives.put(Instant.class, Schema.INSTANT);
    }

    private final Class<T> recordClazz;

    @SuppressWarnings("unchecked")
    public PulsarPrimitiveSchema(Class<T> recordClazz) {
        checkArgument(pulsarPrimitives.containsKey(recordClazz), "Must be of Pulsar primitive types");
        this.recordClazz = recordClazz;
    }

    public static boolean isPulsarPrimitive(Class<?> key) {
        return pulsarPrimitives.containsKey(key);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(recordClazz);
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        Schema<T> schema = (Schema<T>) pulsarPrimitives.get(recordClazz);
        return schema.decode(message);
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(T element) {
        Schema<T> schema = (Schema<T>) pulsarPrimitives.get(recordClazz);
        return schema.encode(element);
    }
}
