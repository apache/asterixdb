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
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

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
    private boolean debug;
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
    }

    private void defineRecordTypes() {
        if (debug) {
            getLog().info("generating debug code");
        }

        typeMap = new HashMap<String, RecordType>();

        RecordType resource = new RecordType("Resource");
        resource.addField("last holder", RecordType.Type.GLOBAL, "-1");
        resource.addField("first waiter", RecordType.Type.GLOBAL, "-1");
        resource.addField("first upgrader", RecordType.Type.GLOBAL, "-1");
        resource.addField("next", RecordType.Type.GLOBAL, null);
        resource.addField("max mode", RecordType.Type.INT, "LockMode.NL");
        resource.addField("dataset id", RecordType.Type.INT, null);
        resource.addField("pk hash val", RecordType.Type.INT, null);
        resource.addField("alloc id", RecordType.Type.SHORT, null);

        resource.addToMap(typeMap);

        RecordType request = new RecordType("Request");
        request.addField("resource id", RecordType.Type.GLOBAL, null);
        request.addField("job slot", RecordType.Type.GLOBAL, null);
        request.addField("prev job request", RecordType.Type.GLOBAL, null);
        request.addField("next job request", RecordType.Type.GLOBAL, null);
        request.addField("next request", RecordType.Type.GLOBAL, null);
        request.addField("lock mode", RecordType.Type.INT, null);
        request.addField("alloc id", RecordType.Type.SHORT, null);

        request.addToMap(typeMap);

        RecordType job = new RecordType("Job");
        job.addField("last holder", RecordType.Type.GLOBAL, "-1");
        job.addField("last waiter", RecordType.Type.GLOBAL, "-1");
        job.addField("last upgrader", RecordType.Type.GLOBAL, "-1");
        job.addField("job id", RecordType.Type.INT, null);
        job.addField("alloc id", RecordType.Type.SHORT, null);

        job.addToMap(typeMap);
    }

    public void execute() throws MojoExecutionException, MojoFailureException {
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        defineRecordTypes();

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

    private void generateSource(Generator.Manager mgrType, String template, String recordType) throws MojoFailureException {
        InputStream is = getClass().getClassLoader().getResourceAsStream(template);
        if (is == null) {
            throw new MojoFailureException("template '" + template + "' not found in classpath");
        }

        StringBuilder sb = new StringBuilder();
        File outputFile = new File(outputDir, recordType + template);

        try {
            getLog().info("generating " + outputFile.toString());

            Generator.generateSource(mgrType, typeMap.get(recordType), is, sb, debug);
            is.close();

            FileWriter outWriter = new FileWriter(outputFile);
            outWriter.write(sb.toString());
            outWriter.close();
        } catch (Exception ex) {
            getLog().error(ex);
            throw new MojoFailureException("failed to generate " + outputFile.toString());
        }
    }
}
