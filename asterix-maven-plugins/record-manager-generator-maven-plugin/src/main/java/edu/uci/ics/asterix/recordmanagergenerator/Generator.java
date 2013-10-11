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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import edu.uci.ics.asterix.recordmanagergenerator.RecordType.Field;

public class Generator {
    
    public enum Manager {
        RECORD,
        ARENA
    }
    
    public static void main(String args[]) {
        
        RecordType resource = new RecordType("Resource");
        resource.addField("last holder",    RecordType.Type.INT, "-1");
        resource.addField("first waiter",   RecordType.Type.INT, "-1");
        resource.addField("first upgrader", RecordType.Type.INT, "-1");
        resource.addField("max mode",       RecordType.Type.INT, "LockMode.NL");
        resource.addField("dataset id",     RecordType.Type.INT, null);
        resource.addField("pk hash val",    RecordType.Type.INT, null);
        resource.addField("next",           RecordType.Type.INT, null);
        
        RecordType request = new RecordType("Request");
        request.addField("resource id",      RecordType.Type.INT,  null);
        request.addField("lock mode",        RecordType.Type.INT,  null);
        request.addField("job id",           RecordType.Type.INT,  null);
        request.addField("prev job request", RecordType.Type.INT,  null);
        request.addField("next job request", RecordType.Type.INT,  null);
        request.addField("next request",     RecordType.Type.INT,  null);

        
        StringBuilder sb = new StringBuilder();

        //generateMemoryManagerSource(request, sb);
        //generateMemoryManagerSource(resource, sb);
        //generateArenaManagerSource(request, sb);
        //generateArenaManagerSource(resource, sb);

        System.out.println(sb.toString());
    }
    
    public static void generateSource(Manager mgr, RecordType rec, InputStream is, StringBuilder sb) {
        switch (mgr) {
            case RECORD:
                generateMemoryManagerSource(rec, is, sb);
                break;
            case ARENA:
                generateArenaManagerSource(rec, is, sb);
                break;
            default:
                throw new IllegalArgumentException();
        }        
    }

    private static void generateMemoryManagerSource(RecordType resource, InputStream is, StringBuilder sb) {
        BufferedReader in = new BufferedReader(new InputStreamReader(is));
        String line = null;

        try {

            String indent = "    ";

            while((line = in.readLine()) != null) {
                if (line.contains("@E@")) {
                    line = line.replace("@E@", resource.name);
                }
                if (line.contains("@CONSTS@")) {
                    resource.appendConstants(sb, indent, 1);
                    sb.append('\n');
                } else if (line.contains("@METHODS@")) {
                    for (int i = 0; i < resource.size(); ++i) {
                        final Field field = resource.fields.get(i);
                        field.appendMemoryManagerGetMethod(sb, indent, 1);
                        sb.append('\n');
                        field.appendMemoryManagerSetMethod(sb, indent, 1);
                        sb.append('\n');
                    }
                } else if (line.contains("@INIT_SLOT@")) {
                    for (int i = 0; i < resource.size(); ++i) {                        
                        final Field field = resource.fields.get(i);
                        field.appendInitializers(sb, indent, 3);
                    }
                } else if (line.contains("@CHECK_SLOT@")) {
                    for (int i = 0; i < resource.size(); ++i) {                        
                        final Field field = resource.fields.get(i);
                        field.appendChecks(sb, indent, 3);
                    }
                } else if (line.contains("@PRINT_BUFFER@")) {
                    resource.appendBufferPrinter(sb, indent, 3);
                    sb.append('\n');
                } else {
                  sb.append(line).append('\n');
                }
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private static void generateArenaManagerSource(RecordType resource, InputStream is, StringBuilder sb) {
        BufferedReader in = new BufferedReader(new InputStreamReader(is));
        String line = null;

        try {

            String indent = "    ";

            while((line = in.readLine()) != null) {
                if (line.contains("@E@")) {
                    line = line.replace("@E@", resource.name);
                }
                if (line.contains("@METHODS@")) {
                    for (int i = 0; i < resource.size(); ++i) {
                        final Field field = resource.fields.get(i);
                        field.appendArenaManagerGetMethod(sb, indent, 1);
                        sb.append('\n');
                        field.appendArenaManagerSetMethod(sb, indent, 1);
                        sb.append('\n');
                    }
                } else {
                  sb.append(line).append('\n');
                }
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
}
