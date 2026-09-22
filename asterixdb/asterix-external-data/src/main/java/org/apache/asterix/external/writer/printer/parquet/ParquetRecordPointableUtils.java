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
package org.apache.asterix.external.writer.printer.parquet;

import static org.apache.asterix.common.exceptions.ErrorCode.TYPE_UNSUPPORTED_PARQUET_WRITE;

import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.lazy.RecordLazyVisitablePointable;
import org.apache.asterix.om.lazy.TypedRecordLazyVisitablePointable;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ParquetRecordPointableUtils {

    private ParquetRecordPointableUtils() {
    }

    /**
     * Resolves the record reader for a writer's source type. Every Parquet writing visitor needs the same answer,
     * and they must not diverge: a type accepted by one and rejected by another fails midway through a write.
     *
     * @param typeInfo the source type as the compiler derived it, which may be a nullable record
     * @return a reader over the record, typed when the layout is known and open otherwise
     * @throws HyracksDataException if the type cannot be written as Parquet. Callers constructed from Parquet's own
     *             API, which declares no checked exception, wrap this in {@code AsterixParquetRuntimeException}.
     */
    public static RecordLazyVisitablePointable createRecordPointable(IAType typeInfo) throws HyracksDataException {
        // A record built by a merge (SELECT s.*, <expr> AS f) is typed as a *nullable* record, since the merge
        // returns unknown when either side is. The writer refuses unknown values per row, so the record type
        // underneath the union is what has to be walked.
        IAType actualType = TypeComputeUtils.getActualType(typeInfo);
        switch (actualType.getTypeTag()) {
            case OBJECT:
                return new TypedRecordLazyVisitablePointable((ARecordType) actualType);
            case ANY:
                return new RecordLazyVisitablePointable(true);
            default:
                throw RuntimeDataException.create(TYPE_UNSUPPORTED_PARQUET_WRITE, actualType.getTypeTag());
        }
    }
}
