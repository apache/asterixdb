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

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import edu.uci.ics.hivesterix.runtime.factory.hashfunction.HiveDoubleBinaryHashFunctionFactory;
import edu.uci.ics.hivesterix.runtime.factory.hashfunction.HiveIntegerBinaryHashFunctionFactory;
import edu.uci.ics.hivesterix.runtime.factory.hashfunction.HiveLongBinaryHashFunctionFactory;
import edu.uci.ics.hivesterix.runtime.factory.hashfunction.HiveRawBinaryHashFunctionFactory;
import edu.uci.ics.hivesterix.runtime.factory.hashfunction.HiveStingBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IBinaryHashFunctionFactoryProvider;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;

public class HiveBinaryHashFunctionFactoryProvider implements IBinaryHashFunctionFactoryProvider {

    public static final HiveBinaryHashFunctionFactoryProvider INSTANCE = new HiveBinaryHashFunctionFactoryProvider();

    private HiveBinaryHashFunctionFactoryProvider() {
    }

    @Override
    public IBinaryHashFunctionFactory getBinaryHashFunctionFactory(Object type) throws AlgebricksException {
        if (type.equals(TypeInfoFactory.intTypeInfo)) {
            return HiveIntegerBinaryHashFunctionFactory.INSTANCE;
        } else if (type.equals(TypeInfoFactory.longTypeInfo)) {
            return HiveLongBinaryHashFunctionFactory.INSTANCE;
        } else if (type.equals(TypeInfoFactory.stringTypeInfo)) {
            return HiveStingBinaryHashFunctionFactory.INSTANCE;
        } else if (type.equals(TypeInfoFactory.doubleTypeInfo)) {
            return HiveDoubleBinaryHashFunctionFactory.INSTANCE;
        } else {
            return HiveRawBinaryHashFunctionFactory.INSTANCE;
        }
    }
}
