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
package org.apache.asterix.dataflow.data.common;

import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface ILogicalBinaryComparator {

    enum Result {
        MISSING,
        NULL,
        EQ,
        LT,
        GT,
        INCOMPARABLE
    }

    static Result asResult(int result) {
        return result < 0 ? Result.LT : (result == 0 ? Result.EQ : Result.GT);
    }

    static boolean inequalityUndefined(ATypeTag tag) {
        switch (tag) {
            case OBJECT:
            case MULTISET:
            case DURATION:
            case INTERVAL:
            case LINE:
            case POINT:
            case POINT3D:
            case POLYGON:
            case CIRCLE:
            case RECTANGLE:
                return true;
            case UNION:
                throw new IllegalArgumentException();
            default:
                return false;
        }
    }

    Result compare(TaggedValueReference left, TaggedValueReference right) throws HyracksDataException;

    Result compare(TaggedValueReference left, IAObject rightConstant);

    Result compare(IAObject leftConstant, TaggedValueReference right);

    Result compare(IAObject leftConstant, IAObject rightConstant);
}
