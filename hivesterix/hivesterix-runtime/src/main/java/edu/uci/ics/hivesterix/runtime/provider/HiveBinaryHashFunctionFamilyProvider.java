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
package edu.uci.ics.hivesterix.runtime.provider;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IBinaryHashFunctionFamilyProvider;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.data.std.accessors.MurmurHash3BinaryHashFunctionFamily;

public class HiveBinaryHashFunctionFamilyProvider implements IBinaryHashFunctionFamilyProvider {

    public static HiveBinaryHashFunctionFamilyProvider INSTANCE = new HiveBinaryHashFunctionFamilyProvider();

    private HiveBinaryHashFunctionFamilyProvider() {

    }

    @Override
    public IBinaryHashFunctionFamily getBinaryHashFunctionFamily(Object type) throws AlgebricksException {
        return MurmurHash3BinaryHashFunctionFamily.INSTANCE;
    }
}
