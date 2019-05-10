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
package org.apache.asterix.formats.nontagged;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.data.IBinaryBooleanInspector;
import org.apache.hyracks.algebricks.data.IBinaryBooleanInspectorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class BinaryBooleanInspector {

    public static final IBinaryBooleanInspectorFactory FACTORY = new IBinaryBooleanInspectorFactory() {

        private static final long serialVersionUID = 1L;

        @Override
        public IBinaryBooleanInspector createBinaryBooleanInspector(IHyracksTaskContext ctx) {
            // Stateless class. No need to construct an object per call
            return BinaryBooleanInspector::getBooleanValue;
        }
    };

    private static final String NAME = "boolean-inspector";

    private BinaryBooleanInspector() {
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    public static boolean getBooleanValue(byte[] bytes, int offset, int length) throws HyracksDataException {
        byte serializedTypeTag = bytes[offset];
        if (serializedTypeTag == ATypeTag.SERIALIZED_MISSING_TYPE_TAG
                || serializedTypeTag == ATypeTag.SERIALIZED_NULL_TYPE_TAG) {
            return false;
        }
        // check if the runtime type is boolean
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serializedTypeTag);
        if (typeTag != ATypeTag.BOOLEAN) {
            throw new RuntimeDataException(ErrorCode.TYPE_MISMATCH_FUNCTION, NAME, 0, ATypeTag.BOOLEAN, typeTag);
        }

        return bytes[offset + 1] == 1;
    }
}
