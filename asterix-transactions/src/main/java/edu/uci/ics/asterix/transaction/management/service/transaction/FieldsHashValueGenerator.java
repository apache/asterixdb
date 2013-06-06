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

package edu.uci.ics.asterix.transaction.management.service.transaction;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public class FieldsHashValueGenerator {
    public static int computeFieldsHashValue(ITupleReference tuple, int[] fieldIndexes,
            IBinaryHashFunction[] fieldHashFunctions) {
        int h = 0;
        for (int i = 0; i < fieldIndexes.length; i++) {
            int primaryKeyFieldIdx = fieldIndexes[i];
            int fh = fieldHashFunctions[i].hash(tuple.getFieldData(primaryKeyFieldIdx),
                    tuple.getFieldStart(primaryKeyFieldIdx), tuple.getFieldLength(primaryKeyFieldIdx));
            h = h * 31 + fh;
            if (h < 0) {
                h = h*(-1);
            }
        }
        return h;
    }
}
