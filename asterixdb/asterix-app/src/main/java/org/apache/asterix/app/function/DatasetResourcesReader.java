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
package org.apache.asterix.app.function;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.asterix.common.context.DatasetResource;
import org.apache.asterix.common.context.IndexInfo;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;

public class DatasetResourcesReader extends FunctionReader {

    private final List<String> components;
    private final Iterator<String> it;
    private final CharArrayRecord record;

    public DatasetResourcesReader(DatasetResource dsr) {
        components = new ArrayList<>();
        if (dsr != null && dsr.isOpen()) {
            Map<Long, IndexInfo> indexes = dsr.getIndexes();
            for (Entry<Long, IndexInfo> entry : indexes.entrySet()) {
                IndexInfo value = entry.getValue();
                ILSMIndex index = value.getIndex();
                components.add(index.toString());
            }
            record = new CharArrayRecord();
        } else {
            record = null;
        }
        it = components.iterator();
    }

    @Override
    public boolean hasNext() throws Exception {
        return it.hasNext();
    }

    @Override
    public IRawRecord<char[]> next() throws IOException, InterruptedException {
        record.reset();
        record.append(it.next().toCharArray());
        record.endRecord();
        return record;
    }
}
