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
package org.apache.asterix.installer.error;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.asterix.event.management.IOutputHandler;
import org.apache.asterix.event.management.OutputAnalysis;
import org.apache.asterix.event.model.EventList.EventType;
import org.apache.asterix.event.schema.pattern.Event;

public class OutputHandler implements IOutputHandler {

    public static IOutputHandler INSTANCE = new OutputHandler();

    private OutputHandler() {

    }

    public OutputAnalysis reportEventOutput(Event event, String output) {

        EventType eventType = EventType.valueOf(event.getType().toUpperCase());
        boolean ignore = true;
        String trimmedOutput = output.trim();
        StringBuffer errorMessage = new StringBuffer();
        switch (eventType) {
            case FILE_TRANSFER:
                if (trimmedOutput.length() > 0) {
                    if (output.contains("Permission denied") || output.contains("cannot find or open")) {
                        ignore = false;
                        break;
                    }
                }
                break;

            case BACKUP:
            case RESTORE:
                if (trimmedOutput.length() > 0) {
                    if (trimmedOutput.contains("AccessControlException")) {
                        errorMessage.append("Insufficient permissions on back up directory");
                        ignore = false;
                    }
                    if (output.contains("does not exist") || output.contains("File exist")
                            || (output.contains("No such file or directory"))) {
                        ignore = true;
                    } else {
                        ignore = false;
                    }
                }
                break;

            case NODE_INFO:
                Properties p = new Properties();
                try {
                    p.load(new ByteArrayInputStream(trimmedOutput.getBytes()));
                } catch (IOException e) {
                }
                String javaVersion = (String) p.get("java_version");
                if (p.get("java_version") == null) {
                    errorMessage.append("Java not installed on " + event.getNodeid().getValue().getAbsvalue());
                    ignore = false;
                } else if (!javaVersion.contains("1.7")) {
                    errorMessage.append("Asterix requires Java 1.7.x. Incompatible version found on  "
                            + event.getNodeid().getValue().getAbsvalue() + "\n");
                    ignore = false;
                }
                break;
        }
        if (ignore) {
            return new OutputAnalysis(true, null);
        } else {
            return new OutputAnalysis(false, errorMessage.toString());
        }
    }
}
