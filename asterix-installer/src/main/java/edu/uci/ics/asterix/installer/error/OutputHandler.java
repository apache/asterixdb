/*
 * Copyright 2009-2012 by The Regents of the University of California
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
package edu.uci.ics.asterix.installer.error;

import edu.uci.ics.asterix.event.management.IOutputHandler;
import edu.uci.ics.asterix.event.management.OutputAnalysis;
import edu.uci.ics.asterix.event.schema.pattern.Event;
import edu.uci.ics.asterix.installer.model.EventList.EventType;

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
                    if (!output.contains("Permission denied") || output.contains("does not exist")
                            || output.contains("File exist")) {
                        ignore = true;
                    } else {
                        ignore = false;
                    }
                }
                break;

            case BACKUP:
                if (trimmedOutput.length() > 0) {
                    if (trimmedOutput.contains("AccessControlException")) {
                        errorMessage.append("Insufficient permissions on HDFS back up directory");
                        ignore = false;
                    }
                    if (output.contains("does not exist") || output.contains("File exist")) {
                        ignore = true;
                    } else {
                        ignore = false;
                    }
                }
                break;

            case RESTORE:
                if (trimmedOutput.length() > 0) {
                    if (trimmedOutput.contains("AccessControlException")) {
                        errorMessage.append("Insufficient permissions on HDFS back up directory");
                        ignore = false;
                    }
                    if (output.contains("does not exist") || output.contains("File exist")) {
                        ignore = true;
                    } else {
                        ignore = false;
                    }
                }
                break;

            case ASTERIX_DEPLOY:
                if (trimmedOutput.length() > 0) {
                    if (trimmedOutput.contains("Exception")) {
                        ignore = false;
                        errorMessage.append("Error in deploying Asterix: " + output);
                        errorMessage.append("\nStop the instance to initiate a cleanup");
                    }
                }
        }
        if (ignore) {
            return new OutputAnalysis(true, null);
        } else {
            return new OutputAnalysis(false, errorMessage.toString());
        }
    }

}
