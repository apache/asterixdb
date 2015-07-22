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
package edu.uci.ics.asterix.external.indexing.input;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.asterix.metadata.external.ExternalFileIndexAccessor;

public class TextFileLookupInputStream extends AbstractHDFSLookupInputStream {

    private HDFSSeekableLineReader lineReader = new HDFSSeekableLineReader();
    private Text value = new Text();

    public TextFileLookupInputStream(ExternalFileIndexAccessor filesIndexAccessor, JobConf conf) throws IOException {
        super(filesIndexAccessor, conf);
    }

    @Override
    public void openFile(String FileName) throws IOException {
        if (lineReader.getReader() != null) {
            lineReader.getReader().close();
        }
        lineReader.resetReader(fs.open(new Path(FileName)));
    }

    @Override
    public void close() {
        if (lineReader.getReader() != null) {
            try {
                lineReader.getReader().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    protected boolean read(long recordOffset) {
        try {
            lineReader.seek(recordOffset);
            lineReader.readLine(value);
            pendingValue = value.toString();
            return true;
        } catch (IOException e) {
            // file was opened and then when trying to seek and read, an error occurred <- should we throw an exception ???->
            e.printStackTrace();
            return false;
        }
    }
}
