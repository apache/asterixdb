package edu.uci.ics.hyracks.examples.text.test;

import java.io.File;

import org.junit.Test;

import edu.uci.ics.hyracks.examples.text.client.WordCountMain;

public class WordCountIT {
    @Test
    public void runWordCount() throws Exception {
        WordCountMain.main(new String[] { "-host", "localhost", "-infile-splits", getInfileSplits(), "-outfile-splits",
                getOutfileSplits(), "-algo", "-hash" });
    }

    private String getInfileSplits() {
        return "NC1:" + new File("data/file1.txt").getAbsolutePath() + ",NC2:"
                + new File("data/file2.txt").getAbsolutePath();
    }

    private String getOutfileSplits() {
        return "NC1:" + new File("target/wc1.txt").getAbsolutePath() + ",NC2:"
                + new File("target/wc2.txt").getAbsolutePath();
    }
}
