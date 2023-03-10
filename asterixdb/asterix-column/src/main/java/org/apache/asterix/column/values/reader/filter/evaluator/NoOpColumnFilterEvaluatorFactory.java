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
package org.apache.asterix.column.values.reader.filter.evaluator;

import org.apache.asterix.column.values.reader.filter.FilterAccessorProvider;
import org.apache.asterix.column.values.reader.filter.IColumnFilterEvaluator;
import org.apache.asterix.column.values.reader.filter.IColumnFilterEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class NoOpColumnFilterEvaluatorFactory implements IColumnFilterEvaluatorFactory {
    private static final long serialVersionUID = -7122361396576592000L;
    public static final IColumnFilterEvaluatorFactory INSTANCE = new NoOpColumnFilterEvaluatorFactory();

    private NoOpColumnFilterEvaluatorFactory() {
    }

    @Override
    public IColumnFilterEvaluator create(FilterAccessorProvider filterAccessorProvider) throws HyracksDataException {
        // True is also NoOp
        return TrueColumnFilterEvaluator.INSTANCE;
    }
}
