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
package org.apache.asterix.testframework.context;

import java.io.File;

public class TestFileContext implements Comparable<TestFileContext> {
    private final File file;

    private String type;

    private int seqNum;

    public TestFileContext(File file) {
        this.file = file;
    }

    public TestFileContext(File file, String type) {
        this.file = file;
        this.type = type;
    }

    public File getFile() {
        return file;
    }

    public String getType() {
        return type;
    }

    public String extension() {
        final String name = file.getName();
        int lastDot = name.lastIndexOf('.');
        return lastDot == -1 ? name : name.substring(lastDot + 1);
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getSeqNum() {
        return seqNum;
    }

    public void setSeqNum(String strSeqNum) {
        seqNum = Integer.parseInt(strSeqNum);
    }

    @Override
    public int compareTo(TestFileContext o) {
        if (this.seqNum > o.seqNum) {
            return 1;
        } else if (this.seqNum < o.seqNum) {
            return -1;
        }
        return 0;
    }

    public TestFileContext copy() {
        TestFileContext copy = new TestFileContext(file);
        copy.setSeqNum(Integer.toString(seqNum));
        copy.setType(type);
        return copy;
    }

    @Override
    public String toString() {
        return String.valueOf(seqNum) + ":" + type + ":" + file;
    }
}
