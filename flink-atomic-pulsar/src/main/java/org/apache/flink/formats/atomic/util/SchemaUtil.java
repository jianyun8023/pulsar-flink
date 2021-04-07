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

package org.apache.flink.formats.atomic.util;

import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.pulsar.client.api.Schema;

/**
 * pulsar schema utils.
 */
public class SchemaUtil {

    public static Schema atomicType2PulsarSchema(AtomicDataType flinkType) throws IllegalArgumentException {
        LogicalTypeRoot type = flinkType.getLogicalType().getTypeRoot();
        switch (type) {
            case BOOLEAN:
                return Schema.BOOL;
            case VARBINARY:
                return Schema.BYTES;
            case DATE:
//                return Schema.LOCAL_DATE;
                // TODO
                return Schema.BYTES;
            case TIME_WITHOUT_TIME_ZONE:
//                return Schema.LOCAL_TIME;
                return Schema.BYTES;
            case VARCHAR:
                return Schema.STRING;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
//                return Schema.LOCAL_DATE_TIME;
                return Schema.BYTES;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
//                return Schema.INSTANT;
                return Schema.BYTES;
            case DOUBLE:
                return Schema.DOUBLE;
            case TINYINT:
                return Schema.INT8;
            case FLOAT:
                return Schema.FLOAT;
            case SMALLINT:
                return Schema.INT16;
            case INTEGER:
                return Schema.INT32;
            case BIGINT:
                return Schema.INT64;
            default:
                throw new IllegalArgumentException(
                    String.format("%s is not supported by Pulsar yet", flinkType.toString()), null);
        }
    }
}
