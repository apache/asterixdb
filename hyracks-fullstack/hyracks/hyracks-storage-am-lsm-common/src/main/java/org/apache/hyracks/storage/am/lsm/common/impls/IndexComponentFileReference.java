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
package org.apache.hyracks.storage.am.lsm.common.impls;

import static org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager.DELIMITER;

import java.util.Objects;

import org.apache.hyracks.api.io.FileReference;

public class IndexComponentFileReference implements Comparable<IndexComponentFileReference> {

    private FileReference fileRef;
    private String fullPath;
    private String fileName;
    private long sequenceStart;
    private long sequenceEnd;

    private IndexComponentFileReference() {
    }

    public static IndexComponentFileReference of(String file) {
        final IndexComponentFileReference ref = new IndexComponentFileReference();
        ref.fileName = file;
        final String[] splits = file.split(DELIMITER);
        ref.sequenceStart = Long.parseLong(splits[0]);
        ref.sequenceEnd = Long.parseLong(splits[1]);
        return ref;
    }

    public static IndexComponentFileReference of(FileReference fileRef) {
        final IndexComponentFileReference ref = of(fileRef.getFile().getName());
        ref.fileRef = fileRef;
        ref.fullPath = fileRef.getFile().getAbsolutePath();
        return ref;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexComponentFileReference that = (IndexComponentFileReference) o;
        return Objects.equals(fileName, that.fileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileName);
    }

    @Override
    public int compareTo(IndexComponentFileReference o) {
        int startCmp = Long.compare(sequenceStart, o.sequenceStart);
        if (startCmp != 0) {
            return startCmp;
        }
        return Long.compare(o.sequenceEnd, sequenceEnd);
    }

    public String getFileName() {
        return fileName;
    }

    public long getSequenceStart() {
        return sequenceStart;
    }

    public long getSequenceEnd() {
        return sequenceEnd;
    }

    public String getFullPath() {
        return fullPath;
    }

    public FileReference getFileRef() {
        return fileRef;
    }

    public String getSequence() {
        return sequenceStart + DELIMITER + sequenceEnd;
    }

    public boolean isMoreRecentThan(IndexComponentFileReference other) {
        return sequenceStart > other.getSequenceEnd();
    }

    public boolean isWithin(IndexComponentFileReference other) {
        return sequenceStart >= other.getSequenceStart() && sequenceEnd <= other.getSequenceEnd();
    }

    @Override
    public String toString() {
        return "{\"type\" : \"" + (isFlush() ? "flush" : "merge") + "\", \"start\" : \"" + sequenceStart
                + "\", \"end\" : \"" + sequenceEnd + "\"}";
    }

    private boolean isFlush() {
        return sequenceStart == sequenceEnd;
    }

    public static String getFlushSequence(long componentSequence) {
        return componentSequence + DELIMITER + componentSequence;
    }

    public static String getMergeSequence(String firstComponentName, String lastComponentName) {
        long mergeSequenceStart = IndexComponentFileReference.of(firstComponentName).getSequenceStart();
        long mergeSequenceEnd = IndexComponentFileReference.of(lastComponentName).getSequenceEnd();
        return mergeSequenceStart + DELIMITER + mergeSequenceEnd;
    }
}
