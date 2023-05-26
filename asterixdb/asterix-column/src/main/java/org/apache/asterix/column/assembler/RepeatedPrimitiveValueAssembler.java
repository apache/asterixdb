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
package org.apache.asterix.column.assembler;

import org.apache.asterix.column.assembler.value.IValueGetter;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.error.ColumnarValueException;

import com.fasterxml.jackson.databind.node.ObjectNode;

class RepeatedPrimitiveValueAssembler extends AbstractPrimitiveValueAssembler {
    private boolean arrayDelegate;

    RepeatedPrimitiveValueAssembler(int level, AssemblerInfo info, IColumnValuesReader reader,
            IValueGetter primitiveValue) {
        super(level, info, reader, primitiveValue);
        this.arrayDelegate = false;
    }

    @Override
    public int next(AssemblerState state) throws HyracksDataException {
        /*
         * Move to the next value if one of the following is true
         * - It is the first time we access this assembler (i.e., the first round)
         * - We are in an array (i.e., the parent array assembler is active)
         * - The value is a delimiter (i.e., the last round)
         */
        if (!state.isInGroup() || reader.isRepeatedValue() || reader.isDelimiter()) {
            next();
        }

        if (isDelegate()) {
            // Indicate to parent that it is the end
            getParent().end();
        }

        //Go to next assembler
        return NEXT_ASSEMBLER;
    }

    public IColumnValuesReader getReader() {
        return reader;
    }

    public void setAsDelegate() {
        // This assembler is responsible for adding null values
        this.arrayDelegate = true;
    }

    private void next() throws HyracksDataException {
        if (!reader.next()) {
            throw createException();
        } else if (reader.isNull() && (arrayDelegate || reader.getLevel() + 1 == level)) {
            /*
             * There are two cases here for where the null belongs to:
             * 1- If the null is an array item, then add it
             * 2- If the null is an ancestor, then we only add null if this column is the array delegate
             * (i.e., arrayDelegate is true)
             */
            addNullToAncestor(reader.getLevel());
        } else if (reader.isMissing() && reader.getLevel() + 1 == level) {
            /*
             * Add a missing item
             */
            addMissingToAncestor(reader.getLevel());
        } else if (reader.isValue()) {
            addValueToParent();
        }
    }

    private ColumnarValueException createException() {
        ColumnarValueException e = new ColumnarValueException();
        ObjectNode assemblerNode = e.createNode(getClass().getSimpleName());
        assemblerNode.put("isDelegate", isDelegate());
        assemblerNode.put("isArrayDelegate", arrayDelegate);

        ObjectNode readerNode = assemblerNode.putObject("assemblerReader");
        reader.appendReaderInformation(readerNode);

        return e;
    }
}
