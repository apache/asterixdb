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

import edu.uci.ics.asterix.dataflow.data.nontagged.keynormalizers.AWrappedAscNormalizedKeyComputerFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.keynormalizers.AWrappedDescNormalizedKeyComputerFactory;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.data.INormalizedKeyComputerFactoryProvider;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.normalizers.DoubleNormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.normalizers.FloatNormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.normalizers.Integer64NormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.normalizers.IntegerNormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.normalizers.UTF8StringNormalizedKeyComputerFactory;

public class AqlNormalizedKeyComputerFactoryProvider implements INormalizedKeyComputerFactoryProvider {

    public static final AqlNormalizedKeyComputerFactoryProvider INSTANCE = new AqlNormalizedKeyComputerFactoryProvider();

    private AqlNormalizedKeyComputerFactoryProvider() {
    }

    @Override
    public INormalizedKeyComputerFactory getNormalizedKeyComputerFactory(Object type, boolean ascending) {
        IAType aqlType = (IAType) type;
        if (ascending) {
            switch (aqlType.getTypeTag()) {
                case DATE:
                case TIME:
                case YEARMONTHDURATION:
                case INT32: {
                    return new AWrappedAscNormalizedKeyComputerFactory(new IntegerNormalizedKeyComputerFactory());
                }
                case DATETIME:
                case DAYTIMEDURATION:
                case INT64: {
                    return new AWrappedAscNormalizedKeyComputerFactory(new Integer64NormalizedKeyComputerFactory());
                }
                case FLOAT: {
                    return new AWrappedAscNormalizedKeyComputerFactory(new FloatNormalizedKeyComputerFactory());
                }
                case DOUBLE: {
                    return new AWrappedAscNormalizedKeyComputerFactory(new DoubleNormalizedKeyComputerFactory());
                }
                case STRING: {
                    return new AWrappedAscNormalizedKeyComputerFactory(new UTF8StringNormalizedKeyComputerFactory());
                }
                default: {
                    return null;
                }
            }
        } else {
            switch (aqlType.getTypeTag()) {
                case DATE:
                case TIME:
                case YEARMONTHDURATION:
                case INT32: {
                    return new AWrappedDescNormalizedKeyComputerFactory(new IntegerNormalizedKeyComputerFactory());
                }
                case DATETIME:
                case DAYTIMEDURATION:
                case INT64: {
                    return new AWrappedDescNormalizedKeyComputerFactory(new Integer64NormalizedKeyComputerFactory());
                }
                case FLOAT: {
                    return new AWrappedDescNormalizedKeyComputerFactory(new FloatNormalizedKeyComputerFactory());
                }
                case DOUBLE: {
                    return new AWrappedDescNormalizedKeyComputerFactory(new DoubleNormalizedKeyComputerFactory());
                }
                case STRING: {
                    return new AWrappedDescNormalizedKeyComputerFactory(new UTF8StringNormalizedKeyComputerFactory());
                }
                default: {
                    return null;
                }
            }
        }
    }
}
