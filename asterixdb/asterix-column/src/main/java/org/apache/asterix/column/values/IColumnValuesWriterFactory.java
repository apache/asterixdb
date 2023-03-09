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
package org.apache.asterix.column.values;

import org.apache.asterix.om.types.ATypeTag;

public interface IColumnValuesWriterFactory {
    /**
     * Create a writer
     *
     * @param tag         column type
     * @param columnIndex column index
     * @param level       maximum level that determine a value is not null or missing
     * @param writeAlways should writer always despite the fact all values were missing/null
     * @param filtered    has a column filter
     * @return a writer
     */
    IColumnValuesWriter createValueWriter(ATypeTag tag, int columnIndex, int level, boolean writeAlways,
            boolean filtered);
}
