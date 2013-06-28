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
package edu.uci.ics.asterix.testframework.context;

import java.io.File;

public class TestFileContext implements Comparable<TestFileContext> {
    private final File file;

    private String type;

    private int seqNum;

    public TestFileContext(File file) {
        this.file = file;
    }

    public File getFile() {
        return file;
    }

    public String getType() {
        return type;
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
        if (this.seqNum > o.seqNum)
            return 1;
        else if (this.seqNum < o.seqNum)
            return -1;
        return 0;
    }
}
