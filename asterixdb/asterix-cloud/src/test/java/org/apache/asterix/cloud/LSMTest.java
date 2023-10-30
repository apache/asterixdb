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
package org.apache.asterix.cloud;

import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

import org.apache.asterix.cloud.clients.ICloudBufferedWriter;
import org.apache.asterix.cloud.clients.ICloudClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class LSMTest {
    public static final Logger LOGGER = LogManager.getLogger();

    public static final String BTREE_SUFFIX = "b";
    public static final String PLAYGROUND_CONTAINER = "playground";
    private final static String BUCKET_STORAGE_ROOT = "storage";
    private static final int BUFFER_SIZE = 136 * 1024 + 5;

    public static ICloudClient CLOUD_CLIENT;

    @Test
    public void a4deleteTest() {
        try {
            CLOUD_CLIENT.deleteObjects(PLAYGROUND_CONTAINER, Collections.singleton(BUCKET_STORAGE_ROOT));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void a1writeToS3Test() throws IOException {
        CloudResettableInputStream stream = null;

        try {
            ICloudBufferedWriter s3BufferedWriter =
                    CLOUD_CLIENT.createBufferedWriter(PLAYGROUND_CONTAINER, BUCKET_STORAGE_ROOT + "/0_b");
            stream = new CloudResettableInputStream(s3BufferedWriter, new WriteBufferProvider(1));
            ByteBuffer content = createContent(BUFFER_SIZE);
            int size = 0;
            for (int i = 0; i < 10; i++) {
                content.clear();
                size += stream.write(content);
            }
            stream.finish();
            System.err.println(size);
        } catch (Exception e) {
            e.printStackTrace();
            if (stream != null) {
                stream.abort();
            }
        } finally {
            if (stream != null) {
                stream.close();
            }
        }
    }

    @Test
    public void a3readFromS3Test() {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
            buffer.clear();

            long offset = BUFFER_SIZE * 4;
            int read = CLOUD_CLIENT.read(PLAYGROUND_CONTAINER, BUCKET_STORAGE_ROOT + "/0_b", offset, buffer);
            buffer.clear();

            for (int i = 0; i < read; i++) {
                assert i % 127 == buffer.get();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void a2listTest() {
        try {
            FilenameFilter btreeFilter = (dir, name) -> !name.startsWith(".") && name.endsWith(BTREE_SUFFIX);
            System.err.println((CLOUD_CLIENT.listObjects(PLAYGROUND_CONTAINER, BUCKET_STORAGE_ROOT, btreeFilter)));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private ByteBuffer createContent(int size) {
        byte[] contentArray = new byte[size];
        for (int i = 0; i < size; i++) {
            contentArray[i] = (byte) (i % 127);
        }
        return ByteBuffer.wrap(contentArray);
    }

}
