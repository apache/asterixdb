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
package org.apache.hyracks.algebricks.core.algebra.visitors;

import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SinkOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteResultOperator;

public interface IQueryOperatorVisitor<R, T> extends ILogicalOperatorVisitor<R, T> {

    @Override
    public default R visitWriteOperator(WriteOperator op, T arg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public default R visitDistributeResultOperator(DistributeResultOperator op, T arg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public default R visitWriteResultOperator(WriteResultOperator op, T arg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public default R visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, T arg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public default R visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, T arg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public default R visitSinkOperator(SinkOperator op, T arg) {
        throw new UnsupportedOperationException();
    }
}
