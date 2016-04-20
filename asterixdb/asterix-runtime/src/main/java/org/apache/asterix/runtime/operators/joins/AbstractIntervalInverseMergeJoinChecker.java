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
package org.apache.asterix.runtime.operators.joins;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;

public abstract class AbstractIntervalInverseMergeJoinChecker extends AbstractIntervalMergeJoinChecker {

    private static final long serialVersionUID = 1L;

    public AbstractIntervalInverseMergeJoinChecker(int idLeft, int idRight) {
        super(idLeft, idRight);
    }

    @Override
    public boolean checkToSaveInMemory(ITupleAccessor accessorLeft, ITupleAccessor accessorRight)
            throws HyracksDataException {
        try {
            IntervalJoinUtil.getIntervalPointable(accessorLeft, idLeft, tvp, ipLeft);
            IntervalJoinUtil.getIntervalPointable(accessorRight, idRight, tvp, ipRight);
            ipLeft.getStart(startLeft);
            ipRight.getEnd(endRight);
            return ch.compare(ipLeft.getTypeTag(), ipRight.getTypeTag(), startLeft, endRight) <= 0;
        } catch (AsterixException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public boolean checkToRemoveInMemory(ITupleAccessor accessorLeft, ITupleAccessor accessorRight)
            throws HyracksDataException {
        return !checkToSaveInMemory(accessorLeft, accessorRight);
    }

}