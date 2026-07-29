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
package org.apache.asterix.runtime.writer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.junit.Test;

public class ExternalFileWriterMaxFileSizeTest {

    private static final int NO_MAX_RESULT = Integer.MAX_VALUE;
    private static final long NO_MAX_FILE_SIZE = -1L;
    private static final String DIRECTORY = "dir/";

    @Test
    public void testSizeUnboundedByDefault() throws HyracksDataException {
        CountingFileWriter fileWriter = new CountingFileWriter(1024);
        write(fileWriter, NO_MAX_RESULT, NO_MAX_FILE_SIZE, 100);
        assertEquals(1, fileWriter.files.size());
    }

    @Test
    public void testRollsOverOnSizeLimit() throws HyracksDataException {
        // 10 objects of 100 bytes each with a 200 byte limit: the limit divides evenly, so each file is
        // closed exactly on it
        CountingFileWriter fileWriter = new CountingFileWriter(100);
        write(fileWriter, NO_MAX_RESULT, 200, 10);
        assertEquals(List.of(2, 2, 2, 2, 2), fileWriter.files);
    }

    @Test
    public void testFileOvershootsWhenObjectCrossesLimit() throws HyracksDataException {
        // 100 byte objects against a 250 byte limit: the third object takes the file to 300 bytes, past the
        // limit, because the limit can only be enforced between objects
        CountingFileWriter fileWriter = new CountingFileWriter(100);
        write(fileWriter, NO_MAX_RESULT, 250, 9);
        assertEquals(List.of(3, 3, 3), fileWriter.files);
    }

    @Test
    public void testCountLimitStillWinsWhenSmaller() throws HyracksDataException {
        CountingFileWriter fileWriter = new CountingFileWriter(1);
        write(fileWriter, 4, 1024, 12);
        assertEquals(List.of(4, 4, 4), fileWriter.files);
    }

    @Test
    public void testSizeLimitStillWinsWhenSmaller() throws HyracksDataException {
        CountingFileWriter fileWriter = new CountingFileWriter(100);
        write(fileWriter, 1000, 200, 9);
        assertEquals(List.of(2, 2, 2, 2, 1), fileWriter.files);
    }

    @Test
    public void testObjectLargerThanLimitIsWrittenAlone() throws HyracksDataException {
        // the limit is consulted before writing, so an oversized object lands in a file of its own that
        // overshoots the limit rather than being split or rejected
        CountingFileWriter fileWriter = new CountingFileWriter(4096);
        write(fileWriter, NO_MAX_RESULT, 1024, 3);
        assertEquals(List.of(1, 1, 1), fileWriter.files);
    }

    @Test
    public void testNoEmptyFileIsCreated() throws HyracksDataException {
        CountingFileWriter fileWriter = new CountingFileWriter(100);
        write(fileWriter, NO_MAX_RESULT, 200, 10);
        for (int objectsInFile : fileWriter.files) {
            assertTrue("a file was created without any object in it", objectsInFile > 0);
        }
    }

    @Test
    public void testSizeIsNotConsultedWhenUnbounded() throws HyracksDataException {
        CountingFileWriter fileWriter = new CountingFileWriter(1024);
        write(fileWriter, NO_MAX_RESULT, NO_MAX_FILE_SIZE, 100);
        assertEquals(0, fileWriter.sizeQueries);
    }

    @Test
    public void testSizeIsConsultedForEveryObject() throws HyracksDataException {
        // the limit is enforced as tightly as it can be: the size is read once per object, so the file can
        // only ever overshoot by the object that crosses the limit
        CountingFileWriter fileWriter = new CountingFileWriter(1);
        int objects = 10000;
        write(fileWriter, NO_MAX_RESULT, 1024 * 1024, objects);
        assertEquals(1, fileWriter.files.size());
        assertEquals(objects, fileWriter.sizeQueries);
    }

    private static void write(CountingFileWriter fileWriter, int maxResultPerFile, long maxFileSize, int objects)
            throws HyracksDataException {
        ExternalFileWriter writer =
                new ExternalFileWriter(new FixedPathResolver(), fileWriter, maxResultPerFile, maxFileSize);
        writer.open();
        writer.initNewPartition(null);
        for (int i = 0; i < objects; i++) {
            writer.write(new VoidPointable());
        }
        writer.close();
    }

    private static class FixedPathResolver implements IPathResolver {
        private int fileCounter;

        @Override
        public String getPartitionDirectory(IFrameTupleReference tuple) {
            return DIRECTORY;
        }

        @Override
        public String getNextFileName() {
            return "file-" + fileCounter++;
        }
    }

    /**
     * Reports a fixed number of bytes per written object and records how many objects landed in each file.
     */
    private static class CountingFileWriter implements IExternalFileWriter {
        private final List<Integer> files = new ArrayList<>();
        private final int bytesPerObject;
        private int sizeQueries;

        CountingFileWriter(int bytesPerObject) {
            this.bytesPerObject = bytesPerObject;
        }

        @Override
        public void open() {
        }

        @Override
        public void validate(String directory) {
        }

        @Override
        public boolean newFile(String directory, String fileName) {
            files.add(0);
            return true;
        }

        @Override
        public void write(IValueReference value) {
            files.set(files.size() - 1, files.get(files.size() - 1) + 1);
        }

        @Override
        public long getBytesWritten() {
            sizeQueries++;
            return (long) files.get(files.size() - 1) * bytesPerObject;
        }

        @Override
        public void abort() {
        }

        @Override
        public void close() {
        }
    }
}
