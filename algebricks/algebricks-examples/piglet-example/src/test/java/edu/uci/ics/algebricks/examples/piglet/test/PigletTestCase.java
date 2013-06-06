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
