/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.runtime.base;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.data.IUnnestingPositionWriter;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.util.annotations.NotThreadSafe;

@NotThreadSafe
final class UnnestingPositionWriter implements IUnnestingPositionWriter {

    private final AMutableInt64 aInt64 = new AMutableInt64(0);

    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<AInt64> aInt64Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);

    @Override
    public void write(DataOutput dataOutput, long position) throws IOException {
        aInt64.setValue(position);
        aInt64Serde.serialize(aInt64, dataOutput);
    }
}
