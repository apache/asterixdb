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

package org.apache.hyracks.dataflow.common.data.normalizers;

import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.dataflow.common.utils.NormalizedKeyUtils;

public class DoubleNormalizedKeyComputerFactoryTest extends AbstractNormalizedKeyComputerFactoryTest {

    private final INormalizedKeyComputer normalizer =
            new DoubleNormalizedKeyComputerFactory().createNormalizedKeyComputer();

    @Override
    protected IPointable getLargePositive() {
        return getDoublePointable(Double.MAX_VALUE - 1);
    }

    @Override
    protected IPointable getSmallPositive() {
        return getDoublePointable(Double.MIN_VALUE);
    }

    @Override
    protected IPointable getLargeNegative() {
        return getDoublePointable(Double.MIN_VALUE * -1);

    }

    @Override
    protected IPointable getSmallNegative() {
        return getDoublePointable(Double.MAX_VALUE * -1);
    }

    private IPointable getDoublePointable(double value) {
        DoublePointable pointable = (DoublePointable) DoublePointable.FACTORY.createPointable();
        pointable.set(new byte[Double.BYTES], 0, Double.BYTES);
        pointable.setDouble(value);
        return pointable;
    }

    @Override
    protected int[] normalize(IPointable value) {
        int[] keys = NormalizedKeyUtils.createNormalizedKeyArray(normalizer);
        normalizer.normalize(value.getByteArray(), value.getStartOffset(), value.getLength(), keys, 0);
        return keys;
    }

}
