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

import org.apache.asterix.column.values.reader.filter.IColumnFilterEvaluator;
import org.apache.asterix.column.values.reader.filter.IColumnFilterEvaluatorFactory;

public abstract class AbstractColumnFilterEvaluatorFactory implements IColumnFilterEvaluatorFactory {
    private static final long serialVersionUID = 1436531448052787426L;

    protected final IColumnFilterEvaluatorFactory left;
    protected final IColumnFilterEvaluatorFactory right;

    public AbstractColumnFilterEvaluatorFactory(IColumnFilterEvaluatorFactory left,
            IColumnFilterEvaluatorFactory right) {
        this.left = left;
        this.right = right;
    }

    protected abstract String getOp();

    @Override
    public String toString() {
        return left.toString() + " " + getOp() + " " + right.toString();
    }

    static abstract class AbstractEvaluator implements IColumnFilterEvaluator {
        protected final IColumnFilterEvaluator left;
        protected final IColumnFilterEvaluator right;

        AbstractEvaluator(IColumnFilterEvaluator left, IColumnFilterEvaluator right) {
            this.left = left;
            this.right = right;
        }
    }
}
