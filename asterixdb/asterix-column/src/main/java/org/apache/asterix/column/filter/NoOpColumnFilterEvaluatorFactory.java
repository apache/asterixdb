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
package org.apache.asterix.column.filter;

import org.apache.asterix.column.filter.iterable.ColumnFilterEvaluatorContext;
import org.apache.asterix.column.filter.iterable.IColumnIterableFilterEvaluator;
import org.apache.asterix.column.filter.iterable.IColumnIterableFilterEvaluatorFactory;
import org.apache.asterix.column.filter.range.IColumnRangeFilterEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * The factory returns {@link TrueColumnFilterEvaluator#INSTANCE} which evaluates always to true
 */
public class NoOpColumnFilterEvaluatorFactory
        implements IColumnRangeFilterEvaluatorFactory, IColumnIterableFilterEvaluatorFactory {
    private static final long serialVersionUID = -7122361396576592000L;
    public static final NoOpColumnFilterEvaluatorFactory INSTANCE = new NoOpColumnFilterEvaluatorFactory();

    private NoOpColumnFilterEvaluatorFactory() {
    }

    @Override
    public IColumnFilterEvaluator create(FilterAccessorProvider filterAccessorProvider) throws HyracksDataException {
        return TrueColumnFilterEvaluator.INSTANCE;
    }

    @Override
    public IColumnIterableFilterEvaluator create(ColumnFilterEvaluatorContext context) throws HyracksDataException {
        return TrueColumnFilterEvaluator.INSTANCE;
    }

    @Override
    public String toString() {
        return "TRUE";
    }
}
