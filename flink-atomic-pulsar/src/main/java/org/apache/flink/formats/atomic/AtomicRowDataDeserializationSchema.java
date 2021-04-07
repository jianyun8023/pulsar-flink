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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.atomic.util.RowDataUtil;
import org.apache.flink.formats.atomic.util.SchemaUtil;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.types.RowKind;

import org.apache.pulsar.client.api.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * rowDeserializationSchema for atomic type.
 */
public class AtomicRowDataDeserializationSchema implements DeserializationSchema<RowData> {
    private static final long serialVersionUID = -228294330688809195L;

    private final AtomicDataType dataType;

    public AtomicRowDataDeserializationSchema(AtomicDataType dataType) {
        this.dataType = dataType;
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        Schema schema = SchemaUtil.atomicType2PulsarSchema(dataType);
        Object data = schema.decode(message);
        final GenericRowData rowData = new GenericRowData(RowKind.INSERT, 1);
        RowDataUtil.setField(rowData, 0, data);
        return rowData;
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        List<DataTypes.Field> mainSchema = new ArrayList<>();
        mainSchema.add(DataTypes.FIELD("value", dataType));
        FieldsDataType fieldsDataType = (FieldsDataType) DataTypes.ROW(mainSchema.toArray(new DataTypes.Field[0]));
        return InternalTypeInfo.of(fieldsDataType.getLogicalType());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AtomicRowDataDeserializationSchema that = (AtomicRowDataDeserializationSchema) o;
        return dataType.equals(that.dataType);
    }

    @Override
    public int hashCode() {
        return 31 * dataType.hashCode();
    }
}
