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

import edu.uci.ics.hivesterix.runtime.factory.comparator.HiveByteBinaryAscComparatorFactory;
import edu.uci.ics.hivesterix.runtime.factory.comparator.HiveByteBinaryDescComparatorFactory;
import edu.uci.ics.hivesterix.runtime.factory.comparator.HiveDoubleBinaryAscComparatorFactory;
import edu.uci.ics.hivesterix.runtime.factory.comparator.HiveDoubleBinaryDescComparatorFactory;
import edu.uci.ics.hivesterix.runtime.factory.comparator.HiveFloatBinaryAscComparatorFactory;
import edu.uci.ics.hivesterix.runtime.factory.comparator.HiveFloatBinaryDescComparatorFactory;
import edu.uci.ics.hivesterix.runtime.factory.comparator.HiveIntegerBinaryAscComparatorFactory;
import edu.uci.ics.hivesterix.runtime.factory.comparator.HiveIntegerBinaryDescComparatorFactory;
import edu.uci.ics.hivesterix.runtime.factory.comparator.HiveLongBinaryAscComparatorFactory;
import edu.uci.ics.hivesterix.runtime.factory.comparator.HiveLongBinaryDescComparatorFactory;
import edu.uci.ics.hivesterix.runtime.factory.comparator.HiveShortBinaryAscComparatorFactory;
import edu.uci.ics.hivesterix.runtime.factory.comparator.HiveShortBinaryDescComparatorFactory;
import edu.uci.ics.hivesterix.runtime.factory.comparator.HiveStringBinaryAscComparatorFactory;
import edu.uci.ics.hivesterix.runtime.factory.comparator.HiveStringBinaryDescComparatorFactory;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;

public class HiveBinaryComparatorFactoryProvider implements IBinaryComparatorFactoryProvider {

    public static final HiveBinaryComparatorFactoryProvider INSTANCE = new HiveBinaryComparatorFactoryProvider();

    private HiveBinaryComparatorFactoryProvider() {
    }

    @Override
    public IBinaryComparatorFactory getBinaryComparatorFactory(Object type, boolean ascending)
            throws AlgebricksException {
        if (type.equals(TypeInfoFactory.intTypeInfo)) {
            if (ascending)
                return HiveIntegerBinaryAscComparatorFactory.INSTANCE;
            else
                return HiveIntegerBinaryDescComparatorFactory.INSTANCE;

        } else if (type.equals(TypeInfoFactory.longTypeInfo)) {
            if (ascending)
                return HiveLongBinaryAscComparatorFactory.INSTANCE;
            else
                return HiveLongBinaryDescComparatorFactory.INSTANCE;

        } else if (type.equals(TypeInfoFactory.floatTypeInfo)) {
            if (ascending)
                return HiveFloatBinaryAscComparatorFactory.INSTANCE;
            else
                return HiveFloatBinaryDescComparatorFactory.INSTANCE;

        } else if (type.equals(TypeInfoFactory.doubleTypeInfo)) {
            if (ascending)
                return HiveDoubleBinaryAscComparatorFactory.INSTANCE;
            else
                return HiveDoubleBinaryDescComparatorFactory.INSTANCE;
        } else if (type.equals(TypeInfoFactory.shortTypeInfo)) {
            if (ascending)
                return HiveShortBinaryAscComparatorFactory.INSTANCE;
            else
                return HiveShortBinaryDescComparatorFactory.INSTANCE;
        } else if (type.equals(TypeInfoFactory.stringTypeInfo)) {
            if (ascending)
                return HiveStringBinaryAscComparatorFactory.INSTANCE;
            else
                return HiveStringBinaryDescComparatorFactory.INSTANCE;
        } else if (type.equals(TypeInfoFactory.byteTypeInfo) || type.equals(TypeInfoFactory.booleanTypeInfo)) {
            if (ascending)
                return HiveByteBinaryAscComparatorFactory.INSTANCE;
            else
                return HiveByteBinaryDescComparatorFactory.INSTANCE;
        } else
            throw new NotImplementedException();
    }
}
