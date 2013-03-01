package edu.uci.ics.algebricks.examples.piglet.test;

import java.io.File;
import java.io.FileReader;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;

import edu.uci.ics.hyracks.algebricks.examples.piglet.ast.ASTNode;
import edu.uci.ics.hyracks.algebricks.examples.piglet.compiler.PigletCompiler;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class PigletTestCase extends TestCase {

    private final File file;

    PigletTestCase(File file) {
        super("testPiglet");
        this.file = file;
    }

    @Test
    public void testPiglet() {
        try {
            FileReader in = new FileReader(file);
            try {
                PigletCompiler c = new PigletCompiler();

                List<ASTNode> ast = c.parse(in);
                JobSpecification jobSpec = c.compile(ast);

                System.err.println(jobSpec.toJSON());
            } finally {
                in.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
