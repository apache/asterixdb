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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.asterix.metadata.external.ExternalFileIndexAccessor;


@SuppressWarnings("deprecation")
public class SequenceFileLookupInputStream extends AbstractHDFSLookupInputStream{

    private SequenceFile.Reader reader;
    private Writable seqKey;
    private Text seqValue = new Text();
    private Configuration conf;
    
    public SequenceFileLookupInputStream(ExternalFileIndexAccessor fileIndexAccessor, JobConf conf) throws IOException{
        super(fileIndexAccessor, conf);
        this.conf = conf;
    }
    
    @Override
    protected void openFile(String fileName) throws IOException {
        if (reader != null) {
            reader.close();
        }
        reader = new SequenceFile.Reader(fs, new Path(fileName), conf);
        seqKey = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf); 
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
            reader.next(seqKey, seqValue);
            pendingValue = seqValue.toString();
            return true;
        } catch (IOException e) {
            // Same Question: seek and read failed afer openning file succeede, should we do something about it?
            e.printStackTrace();
            return false;
        }
    }
}
