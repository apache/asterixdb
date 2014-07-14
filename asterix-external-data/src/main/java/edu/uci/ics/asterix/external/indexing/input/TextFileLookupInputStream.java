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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.asterix.metadata.external.ExternalFileIndexAccessor;


@SuppressWarnings("deprecation")
public class TextFileLookupInputStream extends AbstractHDFSLookupInputStream{

    private FSDataInputStream reader;
    public TextFileLookupInputStream(ExternalFileIndexAccessor filesIndexAccessor, JobConf conf) throws IOException{
        super(filesIndexAccessor, conf);
    }
    @Override
    protected void openFile(String fileName) throws IOException {
        if (reader != null) {
            reader.close();
        }
        reader = fs.open(new Path(fileName));
    }
    
    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
        super.close();
    }
    
    @Override
    protected boolean read(long recordOffset) {
        try {
            reader.seek(recordOffset);
            pendingValue = reader.readLine();
            return true;
        } catch (IOException e) {
            // file was opened and then when trying to seek and read, an error occurred <- should we throw an exception ???->
            e.printStackTrace();
            return false;
        }
    }
}
