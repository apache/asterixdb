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
package org.apache.asterix.runtime.evaluators.common;

import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;

public class EditDistanceContainsEvaluator extends EditDistanceCheckEvaluator {

    public EditDistanceContainsEvaluator(ICopyEvaluatorFactory[] args, IDataOutputProvider output)
            throws AlgebricksException {
        super(args, output);
    }

    @Override
    protected int computeResult(byte[] bytes, int firstStart, int secondStart, ATypeTag argType)
            throws AlgebricksException {
        switch (argType) {

            case STRING: {
                return ed.UTF8StringEditDistanceContains(argOut.getByteArray(), firstStart + typeIndicatorSize,
                        secondStart + typeIndicatorSize, (int) edThresh);
            }

            case ORDEREDLIST: {
                firstOrdListIter.reset(bytes, firstStart);
                secondOrdListIter.reset(bytes, secondStart);
                try {
                    return ed.getSimilarityContains(firstOrdListIter, secondOrdListIter, (int) edThresh);
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }
            }

            default: {
                throw new AlgebricksException(AsterixBuiltinFunctions.EDIT_DISTANCE_CONTAINS.getName()
                        + ": expects input type as STRING or ORDEREDLIST but got " + argType + ".");
            }

        }
    }
}
