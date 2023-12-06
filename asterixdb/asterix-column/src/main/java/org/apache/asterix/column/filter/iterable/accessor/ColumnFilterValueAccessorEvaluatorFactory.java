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
package org.apache.asterix.column.filter.iterable.accessor;

import org.apache.asterix.column.filter.FilterAccessorProvider;
import org.apache.asterix.column.filter.iterable.ColumnFilterEvaluatorContext;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ColumnFilterValueAccessorEvaluatorFactory implements IScalarEvaluatorFactory {
    private static final long serialVersionUID = -7871899093673316190L;
    private final ARecordType path;

    public ColumnFilterValueAccessorEvaluatorFactory(ARecordType path) {
        this.path = path;
    }

    @Override
    public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
        ColumnFilterEvaluatorContext columnEvalCtx = (ColumnFilterEvaluatorContext) ctx;
        FilterAccessorProvider filterAccessorProvider = columnEvalCtx.getFilterAccessorProvider();
        return filterAccessorProvider.createColumnAccessEvaluator(path);
    }
}
