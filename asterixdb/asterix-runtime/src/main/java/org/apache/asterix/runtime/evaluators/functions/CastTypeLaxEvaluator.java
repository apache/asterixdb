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

package org.apache.asterix.runtime.evaluators.functions;

import org.apache.asterix.om.pointables.cast.ACastVisitor;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class CastTypeLaxEvaluator extends CastTypeEvaluator {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final byte[] MISSING_BYTES = new byte[] { ATypeTag.SERIALIZED_MISSING_TYPE_TAG };

    CastTypeLaxEvaluator(IAType reqType, IAType inputType, IScalarEvaluator argEvaluator) {
        super(reqType, inputType, argEvaluator);
    }

    @Override
    protected ACastVisitor createCastVisitor() {
        return new ACastVisitor(false);
    }

    @Override
    protected void cast(IPointable result) {
        try {
            super.cast(result);
        } catch (HyracksDataException e) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.log(Level.TRACE, e.toString(), e);
            }
            result.set(MISSING_BYTES, 0, MISSING_BYTES.length);
        }
    }
}
