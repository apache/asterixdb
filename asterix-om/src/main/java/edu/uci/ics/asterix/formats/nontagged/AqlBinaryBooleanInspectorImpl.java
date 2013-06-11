/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.formats.nontagged;

import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.hyracks.algebricks.data.IBinaryBooleanInspector;
import edu.uci.ics.hyracks.algebricks.data.IBinaryBooleanInspectorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class AqlBinaryBooleanInspectorImpl implements IBinaryBooleanInspector {
    public static final IBinaryBooleanInspectorFactory FACTORY = new IBinaryBooleanInspectorFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public IBinaryBooleanInspector createBinaryBooleanInspector(IHyracksTaskContext ctx) {
            return new AqlBinaryBooleanInspectorImpl();
        }
    };

    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();

    private AqlBinaryBooleanInspectorImpl() {
    }

    @Override
    public boolean getBooleanValue(byte[] bytes, int offset, int length) {
        if (bytes[offset] == SER_NULL_TYPE_TAG)
            return false;
        /** check if the runtime type is boolean */
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[offset]);
        if (typeTag != ATypeTag.BOOLEAN) {
            throw new IllegalStateException("Runtime error: the select condition should be of the boolean type!");
        }
        return bytes[offset + 1] == 1;
    }

}
