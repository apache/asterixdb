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
package org.apache.asterix.column.operation.query;

import java.util.Map;

import org.apache.asterix.column.values.reader.filter.IColumnFilterEvaluatorFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.projection.DataProjectionFiltrationInfo;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.storage.common.projection.ITupleProjector;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;

public class QueryColumnTupleProjectorFactory implements ITupleProjectorFactory {
    private static final long serialVersionUID = 2130283796584264219L;
    private final ARecordType datasetType;
    private final ARecordType metaType;
    private final int numberOfPrimaryKeys;
    private final ARecordType requestedType;
    private final ARecordType requestedMetaType;
    private final Map<String, FunctionCallInformation> functionCallInfo;
    private final Map<String, FunctionCallInformation> metaFunctionCallInfo;
    private final IColumnFilterEvaluatorFactory filterEvaluator;

    public QueryColumnTupleProjectorFactory(ARecordType datasetType, ARecordType metaType, int numberOfPrimaryKeys,
            ARecordType requestedType, Map<String, FunctionCallInformation> functionCallInfo,
            ARecordType requestedMetaType, Map<String, FunctionCallInformation> metaFunctionCallInfo,
            IColumnFilterEvaluatorFactory filterEvaluator) {
        this.datasetType = datasetType;
        this.metaType = metaType;
        this.numberOfPrimaryKeys = numberOfPrimaryKeys;
        this.requestedType = requestedType;
        this.functionCallInfo = functionCallInfo;
        this.requestedMetaType = requestedMetaType;
        this.metaFunctionCallInfo = metaFunctionCallInfo;
        this.filterEvaluator = filterEvaluator;
    }

    @Override
    public ITupleProjector createTupleProjector(IHyracksTaskContext context) throws HyracksDataException {
        IWarningCollector warningCollector = context.getWarningCollector();
        if (requestedMetaType == null
                || DataProjectionFiltrationInfo.EMPTY_TYPE.getTypeName().equals(requestedMetaType.getTypeName())) {
            /*
             * Either the dataset does not contain meta record or none of the meta columns were requested. Thus,
             * ignore reading the meta columns (if exist)
             */
            return new QueryColumnTupleProjector(datasetType, numberOfPrimaryKeys, requestedType, functionCallInfo,
                    filterEvaluator, warningCollector);
        }
        //The query requested some or all of the meta columns
        return new QueryColumnWithMetaTupleProjector(datasetType, metaType, numberOfPrimaryKeys, requestedType,
                functionCallInfo, requestedMetaType, metaFunctionCallInfo, filterEvaluator, warningCollector);
    }
}
