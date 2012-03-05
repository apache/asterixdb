/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.api.aqlj.client;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.logging.Logger;

import edu.uci.ics.asterix.api.aqlj.common.AQLJException;
import edu.uci.ics.asterix.api.aqlj.common.AQLJStream;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * This class supports the buffering of results that are received from the
 * server. The results are buffered completely to a file on disk. The results
 * that are sent back should contain a serde in order to read the results back
 * in. To see the expected format refer to {@link edu.uci.ics.algebricks.runtime.hyracks.writers.SerializedDataWriterFactory} .
 * 
 * @author zheilbron
 */
public class ResultBuffer {
    private static final Logger LOGGER = Logger.getLogger(ResultBuffer.class.getName());

    private static final int BUF_SIZE = 8192;

    private final byte[] buffer;
    private final File tmpFile;
    private final FileOutputStream fos;
    private final FileInputStream fis;
    private final DataInputStream dis;

    private ObjectInputStream ois;
    private ISerializerDeserializer serde;

    public ResultBuffer() throws IOException {
        buffer = new byte[BUF_SIZE];
        tmpFile = File.createTempFile("aqlj", null, new File(System.getProperty("java.io.tmpdir")));
        fos = new FileOutputStream(tmpFile);
        fis = new FileInputStream(tmpFile);
        dis = new DataInputStream(fis);
        serde = null;
    }

    private RecordDescriptor getRecordDescriptor() throws AQLJException {
        RecordDescriptor rd;
        try {
            ois = new ObjectInputStream(fis);
        } catch (IOException e) {
            throw new AQLJException(e);
        }
        try {
            rd = (RecordDescriptor) ois.readObject();
        } catch (IOException e) {
            throw new AQLJException(e);
        } catch (ClassNotFoundException e) {
            throw new AQLJException(e);
        }
        return rd;
    }

    public IAObject get() throws AQLJException {
        Object o;

        if (serde == null) {
            serde = getRecordDescriptor().getFields()[0];
        }

        try {
            o = serde.deserialize(dis);
        } catch (HyracksDataException e) {
            // this is expected behavior... we know when we've reached the end
            // of the
            // results when a EOFException (masked by the HyracksDataException)
            // is thrown
            o = null;
        }

        return (IAObject) o;
    }

    public void appendMessage(AQLJStream aqljStream, long len) throws IOException {
        long pos = 0;
        long read = 0;
        long remaining = 0;

        while (pos < len) {
            remaining = len - pos;
            read = remaining > BUF_SIZE ? BUF_SIZE : remaining;
            aqljStream.receive(buffer, 0, (int) read);
            pos += read;
            fos.write(buffer, 0, (int) read);
        }
    }

    public void close() throws IOException {
        // remove the file!
        if (tmpFile.exists()) {
            tmpFile.delete();
        }
        fos.close();
        fis.close();
        dis.close();
        if (ois != null) {
            ois.close();
        }
    }
}
