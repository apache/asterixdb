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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.column.filter.FilterAccessorProvider;
import org.apache.asterix.column.filter.TrueColumnFilterEvaluator;
import org.apache.asterix.column.filter.iterable.ColumnFilterEvaluatorContext;
import org.apache.asterix.column.filter.iterable.IColumnIterableFilterEvaluator;
import org.apache.asterix.column.filter.iterable.IColumnIterableFilterEvaluatorFactory;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * The pushed-down filter, as the conjuncts of its top-level AND (a single one if it has none). A conjunct reading a
 * path that crosses a collapsed node's map is dropped for the component: its value may sit in a map entry, which no
 * filter reader sees. The others still filter, so one such conjunct does not disable the filter as a whole.
 */
public class ColumnIterableFilterEvaluatorFactory implements IColumnIterableFilterEvaluatorFactory {
    private static final long serialVersionUID = 171140626152211362L;
    private final IScalarEvaluatorFactory[] conjunctFactories;

    public ColumnIterableFilterEvaluatorFactory(IScalarEvaluatorFactory... conjunctFactories) {
        this.conjunctFactories = conjunctFactories;
    }

    @Override
    public IColumnIterableFilterEvaluator create(ColumnFilterEvaluatorContext context) throws HyracksDataException {
        FilterAccessorProvider filterAccessorProvider = context.getFilterAccessorProvider();
        List<IScalarEvaluator> conjuncts = new ArrayList<>(conjunctFactories.length);
        for (IScalarEvaluatorFactory conjunctFactory : conjunctFactories) {
            filterAccessorProvider.clearPathCrossesCollapsedMap();
            // Readers are populated by createScalarEvaluator()
            IScalarEvaluator conjunct = conjunctFactory.createScalarEvaluator(context);
            if (!filterAccessorProvider.pathCrossesCollapsedMap()) {
                conjuncts.add(conjunct);
            }
        }
        List<IColumnValuesReader> readers = filterAccessorProvider.getFilterColumnReaders();
        if (conjuncts.isEmpty()) {
            // Every conjunct may hold a value in a map entry: filter nothing, as if no filter had been pushed down.
            readers.clear();
            return TrueColumnFilterEvaluator.INSTANCE;
        }

        if (readers.isEmpty()) {
            throw new NullPointerException("Readers are empty");
        }

        // Initialize column filter readers by including a primary key level reader
        filterAccessorProvider.initializeFilterReaders();

        IScalarEvaluator evaluator = conjuncts.size() == 1 ? conjuncts.get(0) : new ConjunctionEvaluator(conjuncts);
        if (readers.stream().anyMatch(IColumnValuesReader::isRepeated)) {
            return new ColumnarRepeatedIterableFilterEvaluator(evaluator, readers);
        }
        return new ColumnIterableFilterEvaluator(evaluator, readers);
    }
}
