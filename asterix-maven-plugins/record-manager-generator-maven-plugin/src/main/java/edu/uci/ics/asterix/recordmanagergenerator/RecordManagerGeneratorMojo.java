/*
 * Copyright 2013 by The Regents of the University of California
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

package edu.uci.ics.asterix.recordmanagergenerator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;

/**
 * @goal generate-record-manager
 * @phase generate-sources
 * @requiresDependencyResolution compile
 */
public class RecordManagerGeneratorMojo extends AbstractMojo {
    /**
     * parameter injected from pom.xml
     * 
     * @parameter
     */
    private String arenaManagerTemplate;
    /**
     * parameter injected from pom.xml
     * 
     * @parameter
     */
    private String recordManagerTemplate;
    /**
     * parameter injected from pom.xml
     * 
     * @parameter
     * @required
     */
    private String[] recordTypes;
    /**
     * parameter injected from pom.xml
     * 
     * @parameter
     * @required
     */
    private File outputDir;

    private Map<String, RecordType> typeMap;

    public RecordManagerGeneratorMojo() {
        typeMap = new HashMap<String, RecordType>();

        RecordType resource = new RecordType("Resource");
        resource.addField("last holder", RecordType.Type.INT, "-1");
        resource.addField("first waiter", RecordType.Type.INT, "-1");
        resource.addField("first upgrader", RecordType.Type.INT, "-1");
        resource.addField("max mode", RecordType.Type.INT, "LockMode.NL");
        resource.addField("dataset id", RecordType.Type.INT, null);
        resource.addField("pk hash val", RecordType.Type.INT, null);
        resource.addField("next", RecordType.Type.INT, null);

        typeMap.put(resource.name, resource);

        RecordType request = new RecordType("Request");
        request.addField("resource id", RecordType.Type.INT, null);
        request.addField("lock mode", RecordType.Type.INT, null);
        request.addField("job id", RecordType.Type.INT, null);
        request.addField("prev job request", RecordType.Type.INT, null);
        request.addField("next job request", RecordType.Type.INT, null);
        request.addField("next request", RecordType.Type.INT, null);

        typeMap.put(request.name, request);
    }

    public void execute() throws MojoExecutionException {
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        for (int i = 0; i < recordTypes.length; ++i) {
            final String recordType = recordTypes[i];

            if (recordManagerTemplate != null) {
                generateSource(Generator.Manager.RECORD, recordManagerTemplate, recordType);
            }

            if (arenaManagerTemplate != null) {
                generateSource(Generator.Manager.ARENA, arenaManagerTemplate, recordType);
            }
        }
    }

    private void generateSource(Generator.Manager mgrType, String template, String recordType) {
        try {
            InputStream is = getClass().getClassLoader().getResourceAsStream(template);
            if (is == null) {
                throw new IllegalStateException();
            }

            StringBuilder sb = new StringBuilder();
            Generator.generateSource(mgrType, typeMap.get(recordType), is, sb);

            is.close();

            File outputFile = new File(outputDir, recordType + template);
            getLog().info("generating " + outputFile.toString());

            FileWriter outWriter = new FileWriter(outputFile);
            outWriter.write(sb.toString());
            outWriter.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
}
