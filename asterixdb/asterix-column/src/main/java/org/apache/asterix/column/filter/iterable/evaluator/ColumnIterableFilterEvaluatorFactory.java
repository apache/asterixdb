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
package org.apache.asterix.column.filter.iterable.evaluator;

import java.util.List;

import org.apache.asterix.column.filter.FilterAccessorProvider;
import org.apache.asterix.column.filter.iterable.ColumnFilterEvaluatorContext;
import org.apache.asterix.column.filter.iterable.IColumnIterableFilterEvaluator;
import org.apache.asterix.column.filter.iterable.IColumnIterableFilterEvaluatorFactory;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ColumnIterableFilterEvaluatorFactory implements IColumnIterableFilterEvaluatorFactory {
    private static final long serialVersionUID = 171140626152211361L;
    private final IScalarEvaluatorFactory evaluatorFactory;

    public ColumnIterableFilterEvaluatorFactory(IScalarEvaluatorFactory evaluatorFactory) {
        this.evaluatorFactory = evaluatorFactory;
    }

    @Override
    public IColumnIterableFilterEvaluator create(ColumnFilterEvaluatorContext context) throws HyracksDataException {
        IScalarEvaluator evaluator = evaluatorFactory.createScalarEvaluator(context);
        FilterAccessorProvider filterAccessorProvider = context.getFilterAccessorProvider();
        // Readers are populated by evaluatorFactory.createScalarEvaluator()
        List<IColumnValuesReader> readers = filterAccessorProvider.getFilterColumnReaders();

        if (readers.isEmpty()) {
            throw new NullPointerException("Readers are empty");
        }

        if (readers.stream().anyMatch(IColumnValuesReader::isRepeated)) {
            return new ColumnarRepeatedIterableFilterEvaluator(evaluator, readers);
        }
        return new ColumnIterableFilterEvaluator(evaluator, readers);
    }
}
