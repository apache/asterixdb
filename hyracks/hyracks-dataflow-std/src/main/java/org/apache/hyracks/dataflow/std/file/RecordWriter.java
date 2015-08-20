/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.file;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import edu.uci.ics.hyracks.dataflow.std.util.StringSerializationUtils;

public abstract class RecordWriter implements IRecordWriter {

    protected final BufferedWriter bufferedWriter;
    protected final int[] columns;
    protected final char separator;

    public static final char COMMA = ',';

    public RecordWriter(Object[] args) throws Exception {
        OutputStream outputStream = createOutputStream(args);
        if (outputStream != null) {
            bufferedWriter = new BufferedWriter(new OutputStreamWriter(createOutputStream(args)));
        } else {
            bufferedWriter = null;
        }
        this.columns = null;
        this.separator = COMMA;
    }

    public RecordWriter(int[] columns, char separator, Object[] args) throws Exception {
        OutputStream outputStream = createOutputStream(args);
        if (outputStream != null) {
            bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));
        } else {
            bufferedWriter = null;
        }
        this.columns = columns;
        this.separator = separator;
    }

    @Override
    public void close() {
        try {
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Object[] record) throws Exception {
        if (columns == null) {
            for (int i = 0; i < record.length; ++i) {
                if (i != 0) {
                    bufferedWriter.write(separator);
                }
                bufferedWriter.write(StringSerializationUtils.toString(record[i]));
            }
        } else {
            for (int i = 0; i < columns.length; ++i) {
                if (i != 0) {
                    bufferedWriter.write(separator);
                }
                bufferedWriter.write(StringSerializationUtils.toString(record[columns[i]]));
            }
        }
        bufferedWriter.write("\n");
    }

    public abstract OutputStream createOutputStream(Object[] args) throws Exception;

}
