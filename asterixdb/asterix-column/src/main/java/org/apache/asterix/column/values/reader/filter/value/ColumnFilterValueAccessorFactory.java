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
package org.apache.asterix.column.values.reader.filter.value;

import org.apache.asterix.column.values.reader.filter.FilterAccessorProvider;
import org.apache.asterix.column.values.reader.filter.IColumnFilterValueAccessor;
import org.apache.asterix.column.values.reader.filter.IColumnFilterValueAccessorFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.visitor.PathStringBuilderForIATypeVisitor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ColumnFilterValueAccessorFactory implements IColumnFilterValueAccessorFactory {
    private static final long serialVersionUID = -6341611172763952841L;
    private final ARecordType path;
    private final boolean min;

    public ColumnFilterValueAccessorFactory(ARecordType path, boolean min) {
        this.path = path;
        this.min = min;
    }

    @Override
    public IColumnFilterValueAccessor create(FilterAccessorProvider filterAccessorProvider)
            throws HyracksDataException {
        return filterAccessorProvider.createColumnFilterValueAccessor(path, min);
    }

    @Override
    public String toString() {
        PathStringBuilderForIATypeVisitor pathBuilder = new PathStringBuilderForIATypeVisitor();
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(min ? "min" : "max");
        stringBuilder.append('(');
        path.accept(pathBuilder, stringBuilder);
        stringBuilder.append(')');

        return stringBuilder.toString();
    }

}
