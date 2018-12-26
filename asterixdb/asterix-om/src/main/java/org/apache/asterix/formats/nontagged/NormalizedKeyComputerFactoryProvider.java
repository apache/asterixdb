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

import org.apache.asterix.dataflow.data.nontagged.keynormalizers.AUUIDNormalizedKeyComputerFactory;
import org.apache.asterix.dataflow.data.nontagged.keynormalizers.AWrappedAscNormalizedKeyComputerFactory;
import org.apache.asterix.dataflow.data.nontagged.keynormalizers.AWrappedDescNormalizedKeyComputerFactory;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.data.INormalizedKeyComputerFactoryProvider;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.dataflow.common.data.normalizers.ByteArrayNormalizedKeyComputerFactory;
import org.apache.hyracks.dataflow.common.data.normalizers.DoubleNormalizedKeyComputerFactory;
import org.apache.hyracks.dataflow.common.data.normalizers.FloatNormalizedKeyComputerFactory;
import org.apache.hyracks.dataflow.common.data.normalizers.Integer64NormalizedKeyComputerFactory;
import org.apache.hyracks.dataflow.common.data.normalizers.IntegerNormalizedKeyComputerFactory;
import org.apache.hyracks.dataflow.common.data.normalizers.UTF8StringNormalizedKeyComputerFactory;

public class NormalizedKeyComputerFactoryProvider implements INormalizedKeyComputerFactoryProvider {

    public static final NormalizedKeyComputerFactoryProvider INSTANCE = new NormalizedKeyComputerFactoryProvider();

    private NormalizedKeyComputerFactoryProvider() {
    }

    @Override
    public INormalizedKeyComputerFactory getNormalizedKeyComputerFactory(Object typeInfo, boolean ascending) {
        IAType type = (IAType) typeInfo;
        if (ascending) {
            switch (type.getTypeTag()) {
                case DATE:
                case TIME:
                case YEARMONTHDURATION:
                case INTEGER:
                    return new AWrappedAscNormalizedKeyComputerFactory(new IntegerNormalizedKeyComputerFactory());
                case DATETIME:
                case DAYTIMEDURATION:
                case BIGINT:
                    return new AWrappedAscNormalizedKeyComputerFactory(new Integer64NormalizedKeyComputerFactory());
                case FLOAT:
                    return new AWrappedAscNormalizedKeyComputerFactory(new FloatNormalizedKeyComputerFactory());
                case DOUBLE:
                    return new AWrappedAscNormalizedKeyComputerFactory(new DoubleNormalizedKeyComputerFactory());
                case STRING:
                    return new AWrappedAscNormalizedKeyComputerFactory(new UTF8StringNormalizedKeyComputerFactory());
                case BINARY:
                    return new AWrappedAscNormalizedKeyComputerFactory(new ByteArrayNormalizedKeyComputerFactory());
                case UUID:
                    return new AWrappedAscNormalizedKeyComputerFactory(new AUUIDNormalizedKeyComputerFactory());
                default:
                    return null;
            }
        } else {
            switch (type.getTypeTag()) {
                case DATE:
                case TIME:
                case YEARMONTHDURATION:
                case INTEGER:
                    return new AWrappedDescNormalizedKeyComputerFactory(new IntegerNormalizedKeyComputerFactory());
                case DATETIME:
                case DAYTIMEDURATION:
                case BIGINT:
                    return new AWrappedDescNormalizedKeyComputerFactory(new Integer64NormalizedKeyComputerFactory());
                case FLOAT:
                    return new AWrappedDescNormalizedKeyComputerFactory(new FloatNormalizedKeyComputerFactory());
                case DOUBLE:
                    return new AWrappedDescNormalizedKeyComputerFactory(new DoubleNormalizedKeyComputerFactory());
                case STRING:
                    return new AWrappedDescNormalizedKeyComputerFactory(new UTF8StringNormalizedKeyComputerFactory());
                case BINARY:
                    return new AWrappedDescNormalizedKeyComputerFactory(new ByteArrayNormalizedKeyComputerFactory());
                case UUID:
                    return new AWrappedDescNormalizedKeyComputerFactory(new AUUIDNormalizedKeyComputerFactory());
                default:
                    return null;
            }
        }
    }
}
