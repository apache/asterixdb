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

public class PrimitiveValueAssembler extends AbstractPrimitiveValueAssembler {

    PrimitiveValueAssembler(int level, AssemblerInfo info, IColumnValuesReader reader, IValueGetter primitiveValue) {
        super(level, info, reader, primitiveValue);
    }

    @Override
    public int next() throws HyracksDataException {
        if (!reader.next()) {
            throw new IllegalAccessError("no more values");
        } else if (reader.isNull() && (isDelegate() || reader.getLevel() + 1 == level)) {
            addNullToAncestor(reader.getLevel());
        } else if (reader.isValue()) {
            addValueToParent();
        }

        if (isDelegate()) {
            getParent().end();
        }
        //Go to next value
        return -1;
    }
}
