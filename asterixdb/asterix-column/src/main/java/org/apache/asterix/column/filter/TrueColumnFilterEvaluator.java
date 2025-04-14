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

import org.apache.asterix.column.filter.iterable.IColumnIterableFilterEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.error.ColumnarValueException;

/**
 * This evaluator is also used to indicate a NoOp filter
 */
public class TrueColumnFilterEvaluator implements IColumnIterableFilterEvaluator {
    public static final IColumnIterableFilterEvaluator INSTANCE = new TrueColumnFilterEvaluator();

    private TrueColumnFilterEvaluator() {
    }

    @Override
    public boolean evaluate() throws HyracksDataException {
        return true;
    }

    @Override
    public void reset(int tupleCount) {
        // NoOp
    }

    @Override
    public int getTupleIndex() {
        return -1;
    }

    @Override
    public int getValueIndex() {
        return -1;
    }

    @Override
    public void setAt(int index) {
        // NoOp
    }

    @Override
    public void appendInformation(ColumnarValueException e) {
        // NoOp
    }
}
