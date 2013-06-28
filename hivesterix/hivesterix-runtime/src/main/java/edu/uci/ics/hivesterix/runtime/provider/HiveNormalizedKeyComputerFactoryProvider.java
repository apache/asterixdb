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

import edu.uci.ics.hivesterix.runtime.factory.normalize.HiveDoubleAscNormalizedKeyComputerFactory;
import edu.uci.ics.hivesterix.runtime.factory.normalize.HiveDoubleDescNormalizedKeyComputerFactory;
import edu.uci.ics.hivesterix.runtime.factory.normalize.HiveIntegerAscNormalizedKeyComputerFactory;
import edu.uci.ics.hivesterix.runtime.factory.normalize.HiveIntegerDescNormalizedKeyComputerFactory;
import edu.uci.ics.hivesterix.runtime.factory.normalize.HiveLongAscNormalizedKeyComputerFactory;
import edu.uci.ics.hivesterix.runtime.factory.normalize.HiveLongDescNormalizedKeyComputerFactory;
import edu.uci.ics.hivesterix.runtime.factory.normalize.HiveStringAscNormalizedKeyComputerFactory;
import edu.uci.ics.hivesterix.runtime.factory.normalize.HiveStringDescNormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.algebricks.data.INormalizedKeyComputerFactoryProvider;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;

public class HiveNormalizedKeyComputerFactoryProvider implements INormalizedKeyComputerFactoryProvider {

    public static final HiveNormalizedKeyComputerFactoryProvider INSTANCE = new HiveNormalizedKeyComputerFactoryProvider();

    private HiveNormalizedKeyComputerFactoryProvider() {
    }

    @Override
    public INormalizedKeyComputerFactory getNormalizedKeyComputerFactory(Object type, boolean ascending) {
        if (ascending) {
            if (type.equals(TypeInfoFactory.stringTypeInfo)) {
                return new HiveStringAscNormalizedKeyComputerFactory();
            } else if (type.equals(TypeInfoFactory.intTypeInfo)) {
                return new HiveIntegerAscNormalizedKeyComputerFactory();
            } else if (type.equals(TypeInfoFactory.longTypeInfo)) {
                return new HiveLongAscNormalizedKeyComputerFactory();
            } else if (type.equals(TypeInfoFactory.doubleTypeInfo)) {
                return new HiveDoubleAscNormalizedKeyComputerFactory();
            } else {
                return null;
            }
        } else {
            if (type.equals(TypeInfoFactory.stringTypeInfo)) {
                return new HiveStringDescNormalizedKeyComputerFactory();
            } else if (type.equals(TypeInfoFactory.intTypeInfo)) {
                return new HiveIntegerDescNormalizedKeyComputerFactory();
            } else if (type.equals(TypeInfoFactory.longTypeInfo)) {
                return new HiveLongDescNormalizedKeyComputerFactory();
            } else if (type.equals(TypeInfoFactory.doubleTypeInfo)) {
                return new HiveDoubleDescNormalizedKeyComputerFactory();
            } else {
                return null;
            }
        }
    }
}
