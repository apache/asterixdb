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
