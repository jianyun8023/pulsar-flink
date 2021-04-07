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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.atomic.util.RowDataUtil;
import org.apache.flink.formats.atomic.util.SchemaUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;

import java.util.Objects;

/**
 * rowSerializationSchema for atomic type.
 */
public class AtomicRowDataSerializationSchema implements SerializationSchema<RowData> {
    private static final long serialVersionUID = -2885556750743978636L;

    private final AtomicDataType atomicType;

    public AtomicRowDataSerializationSchema(AtomicDataType atomicType) {
        this.atomicType = atomicType;
    }

    @Override
    public byte[] serialize(RowData row) {
        Object value = RowDataUtil.getField(row, 0, atomicType.getLogicalType().getDefaultConversion());
        return SchemaUtil.atomicType2PulsarSchema(atomicType).encode(value);
    }

    public DataType getAtomicType() {
        return atomicType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AtomicRowDataSerializationSchema that = (AtomicRowDataSerializationSchema) o;
        return Objects.equals(atomicType, that.atomicType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(atomicType);
    }
}
