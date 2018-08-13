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

import java.io.Serializable;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.data.ILinearizeComparatorFactoryProvider;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import org.apache.hyracks.storage.am.rtree.linearize.HilbertDoubleComparatorFactory;
import org.apache.hyracks.storage.am.rtree.linearize.ZCurveDoubleComparatorFactory;
import org.apache.hyracks.storage.am.rtree.linearize.ZCurveIntComparatorFactory;

public class LinearizeComparatorFactoryProvider implements ILinearizeComparatorFactoryProvider, Serializable {

    private static final long serialVersionUID = 1L;
    public static final LinearizeComparatorFactoryProvider INSTANCE = new LinearizeComparatorFactoryProvider();

    private LinearizeComparatorFactoryProvider() {
    }

    @Override
    public ILinearizeComparatorFactory getLinearizeComparatorFactory(Object type, boolean ascending, int dimension)
            throws AlgebricksException {
        ATypeTag typeTag = (ATypeTag) type;

        if (dimension == 2 && (typeTag == ATypeTag.DOUBLE)) {
            return addOffset(new HilbertDoubleComparatorFactory(2), ascending);
        } else if (typeTag == ATypeTag.DOUBLE) {
            return addOffset(new ZCurveDoubleComparatorFactory(dimension), ascending);
        } else if (typeTag == ATypeTag.TINYINT || typeTag == ATypeTag.SMALLINT || typeTag == ATypeTag.INTEGER
                || typeTag == ATypeTag.BIGINT) {
            return addOffset(new ZCurveIntComparatorFactory(dimension), ascending);
        } else {
            throw new AlgebricksException(
                    "Cannot propose linearizer for key with type " + typeTag + " and dimension " + dimension + ".");
        }
    }

    private ILinearizeComparatorFactory addOffset(final IBinaryComparatorFactory inst, final boolean ascending) {
        return new OrderedLinearizeComparatorFactory(inst, ascending);
    }
}
