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
package org.apache.asterix.app.result.fields;

import java.io.PrintWriter;

import org.apache.asterix.api.http.server.ResultUtil;
import org.apache.asterix.common.api.IResponseFieldPrinter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.result.ResultDirectoryRecord;

public class PartitionInfoPrinter implements IResponseFieldPrinter {
    public static final String FIELD_NAME = "partitions";
    public static final String HANDLE_FIELD_NAME = "handle";
    public static final String RESULT_COUNT_FIELD_NAME = "resultCount";
    public static final String RESULTSET_ORDERED_FIELD_NAME = "resultSetOrdered";

    private final ResultDirectoryRecord[] resultRecords;
    private final String handlePrefix;
    private final boolean resultSetOrdered;

    public PartitionInfoPrinter(ResultDirectoryRecord[] resultRecords, String handlePrefix, boolean resultSetOrdered) {
        this.resultRecords = resultRecords;
        this.handlePrefix = handlePrefix;
        this.resultSetOrdered = resultSetOrdered;
    }

    @Override
    public void print(PrintWriter pw) throws HyracksDataException {
        pw.print("\t\"");
        pw.print(getName());
        pw.print("\": [");
        for (int i = 0; i < resultRecords.length; i++) {
            final ResultDirectoryRecord record = resultRecords[i];
            pw.print("{ \n\t");
            ResultUtil.printField(pw, HANDLE_FIELD_NAME, handlePrefix + "/" + i);
            pw.print("\n\t");
            ResultUtil.printField(pw, RESULT_COUNT_FIELD_NAME, record.getResultCount(), false);
            pw.print("\n\t} \n\t");
            if (i < resultRecords.length - 1) {
                pw.print(",");
            }
        }
        pw.print("],");
        pw.print("\n\t");
        ResultUtil.printField(pw, RESULTSET_ORDERED_FIELD_NAME, resultSetOrdered, false);
    }

    @Override
    public String getName() {
        return FIELD_NAME;
    }
}
