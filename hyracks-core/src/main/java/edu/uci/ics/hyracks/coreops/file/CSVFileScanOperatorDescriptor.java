/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.coreops.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class CSVFileScanOperatorDescriptor extends
        AbstractFileScanOperatorDescriptor {
    private static class CSVRecordReaderImpl implements IRecordReader {
        private final BufferedReader in;
        private final char separator;
        private final String quotes;

        CSVRecordReaderImpl(File file, RecordDescriptor desc, char separator,
                String quotes) throws Exception {
            in = new BufferedReader(new InputStreamReader(new FileInputStream(
                    file)));
            this.separator = separator;
            this.quotes = quotes;
        }

        @Override
        public void close() {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public boolean read(Object[] record) throws Exception {
            String line = in.readLine();
            if (line == null) {
                return false;
            }
            int fid = 0;
            char[] chars = line.toCharArray();
            int i = 0;
            boolean insideQuote = false;
            char quoteChar = 0;
            int partStart = 0;
            boolean skipNext = false;
            while (fid < record.length && i < chars.length) {
                char c = chars[i];
                if (!skipNext) {
                    if (quotes.indexOf(c) >= 0) {
                        if (insideQuote) {
                            if (quoteChar == c) {
                                insideQuote = false;
                            }
                        } else {
                            insideQuote = true;
                            quoteChar = c;
                        }
                    } else if (c == separator) {
                        if (!insideQuote) {
                            record[fid++] = String.valueOf(chars, partStart, i
                                    - partStart);
                            partStart = i + 1;
                        }
                    } else if (c == '\\') {
                        skipNext = true;
                    }
                } else {
                    skipNext = false;
                }
                ++i;
            }
            if (fid < record.length) {
                record[fid] = String.valueOf(chars, partStart, i - partStart);
            }
            return true;
        }
    }

    private static final long serialVersionUID = 1L;

    private final char separator;
    private final String quotes;

    public CSVFileScanOperatorDescriptor(JobSpecification spec,
            FileSplit[] splits, RecordDescriptor recordDescriptor) {
        this(spec, splits, recordDescriptor, ',', "'\"");
    }

    public CSVFileScanOperatorDescriptor(JobSpecification spec,
            FileSplit[] splits, RecordDescriptor recordDescriptor,
            char separator, String quotes) {
        super(spec, splits, recordDescriptor);
        this.separator = separator;
        this.quotes = quotes;
    }

    @Override
    protected IRecordReader createRecordReader(File file, RecordDescriptor desc)
            throws Exception {
        return new CSVRecordReaderImpl(file, desc, separator, quotes);
    }

	@Override
	protected void configure() throws Exception {
		// currently a no-op, but is meant to initialize , if required before it is asked 
		// to create a record reader
		// this is executed at the node and is useful for operators that could not be 
		// initialized from the client completely, because of lack of information specific 
		// to the node where the operator gets executed. 
		
	}
}