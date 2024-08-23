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

package org.apache.asterix.app.function.collectionsize;

import java.io.IOException;

import org.apache.asterix.app.function.FunctionReader;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.input.record.CharArrayRecord;

public class StorageSizeReader extends FunctionReader {

    private final long size;
    private final CharArrayRecord record;
    private boolean hasNext = true;

    StorageSizeReader(long size) {
        this.size = size;
        record = new CharArrayRecord();
    }

    @Override
    public boolean hasNext() throws IOException {
        return hasNext;
    }

    @Override
    public IRawRecord<char[]> next() throws IOException {
        hasNext = false;
        record.reset();
        String result = "{\"size\":" + size + "}";
        record.append(result.toCharArray());
        record.endRecord();
        return record;
    }
}
