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
package org.apache.asterix.external.input.record.reader.hdfs.parquet;

import java.io.DataOutput;

import org.apache.asterix.external.parser.jackson.ParserContext;
import org.apache.parquet.schema.GroupType;

class RepeatedConverter extends AbstractComplexConverter {
    public RepeatedConverter(AbstractComplexConverter parent, int index, GroupType parquetType, ParserContext context) {
        super(parent, index, parquetType, context);
    }

    @Override
    public void start() {
        //NoOp
    }

    @Override
    public void end() {
        //NoOp
    }

    @Override
    protected void addValue(IFieldValue value) {
        parent.addValue(value);
    }

    @Override
    protected AtomicConverter createAtomicConverter(GroupType type, int index) {
        return new AtomicConverter(this, index, context);
    }

    @Override
    protected ArrayConverter createArrayConverter(GroupType type, int index) {
        final GroupType arrayType = type.getType(index).asGroupType();
        return new ArrayConverter(this, index, arrayType, context);
    }

    @Override
    protected ObjectConverter createObjectConverter(GroupType type, int index) {
        return new ObjectConverter(this, index, type.getType(index).asGroupType(), context);
    }

    @Override
    protected DataOutput getDataOutput() {
        return getParentDataOutput();
    }
}
