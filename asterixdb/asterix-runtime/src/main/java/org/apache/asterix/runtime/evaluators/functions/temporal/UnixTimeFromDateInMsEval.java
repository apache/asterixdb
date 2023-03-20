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

package org.apache.asterix.runtime.evaluators.functions.temporal;

import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.SourceLocation;

final class UnixTimeFromDateInMsEval extends AbstractUnixTimeFromDateEval {

    public static final long MS_IN_DAY = 86400000l;

    UnixTimeFromDateInMsEval(IScalarEvaluator arg0, SourceLocation sourceLoc, FunctionIdentifier functionIdentifier) {
        super(arg0, sourceLoc, functionIdentifier);
    }

    @Override
    long chrononToUnixTime(long chronon) {
        return chronon * MS_IN_DAY;
    }
}
