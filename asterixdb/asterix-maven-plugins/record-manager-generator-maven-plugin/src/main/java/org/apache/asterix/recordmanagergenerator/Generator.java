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

package org.apache.asterix.recordmanagergenerator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.asterix.recordmanagergenerator.RecordType.Field;

public class Generator {

    public enum TemplateType {
        RECORD_MANAGER,
        ARENA_MANAGER,
        SUPPORT
    }

    public static void generateSource(TemplateType tmplType, String packageName, RecordType rec, InputStream is,
            StringBuilder sb, boolean debug) {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(is));

            switch (tmplType) {
                case RECORD_MANAGER:
                    generateMemoryManagerSource(packageName, rec, in, sb, debug);
                    break;
                case ARENA_MANAGER:
                    generateArenaManagerSource(packageName, rec, in, sb, debug);
                    break;
                case SUPPORT:
                    generateSupportFileSource(packageName, in, sb, debug);
                    break;
                default:
                    throw new IllegalArgumentException();
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

    }

    private static void generateMemoryManagerSource(String packageName, RecordType resource, BufferedReader in,
            StringBuilder sb, boolean debug) throws IOException {
        String line = null;
        String indent = "    ";

        while ((line = in.readLine()) != null) {
            if (line.contains("@PACKAGE@")) {
                line = line.replace("@PACKAGE@", packageName);
            }
            if (line.contains("@E@")) {
                line = line.replace("@E@", resource.name);
            }
            if (line.contains("@DEBUG@")) {
                line = line.replace("@DEBUG@", Boolean.toString(debug));
            }
            if (line.contains("@CONSTS@")) {
                resource.appendConstants(sb, indent, 1);
                sb.append('\n');
            } else if (line.contains("@METHODS@")) {
                for (int i = 0; i < resource.size(); ++i) {
                    final Field field = resource.fields.get(i);
                    if (field.accessible) {
                        field.appendMemoryManagerGetMethod(sb, indent, 1);
                        sb.append('\n');
                        field.appendMemoryManagerSetMethod(sb, indent, 1);
                        sb.append('\n');
                    }
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
    }

    private static void generateArenaManagerSource(String packageName, RecordType resource, BufferedReader in,
            StringBuilder sb, boolean debug) throws IOException {
        String line = null;
        String indent = "    ";

        while ((line = in.readLine()) != null) {
            if (line.contains("@PACKAGE@")) {
                line = line.replace("@PACKAGE@", packageName);
            }
            if (line.contains("@E@")) {
                line = line.replace("@E@", resource.name);
            }
            if (line.contains("@DEBUG@")) {
                line = line.replace("@DEBUG@", Boolean.toString(debug));
            }
            if (line.contains("@METHODS@")) {
                for (int i = 0; i < resource.size(); ++i) {
                    final Field field = resource.fields.get(i);
                    if (field.accessible) {
                        field.appendArenaManagerGetMethod(sb, indent, 1);
                        sb.append('\n');
                        field.appendArenaManagerSetMethod(sb, indent, 1);
                        sb.append('\n');
                    }
                }
            } else if (line.contains("@PRINT_RECORD@")) {
                resource.appendRecordPrinter(sb, indent, 1);
                sb.append('\n');
            } else {
                sb.append(line).append('\n');
            }
        }
    }

    private static void generateSupportFileSource(String packageName, BufferedReader in, StringBuilder sb,
            boolean debug) throws IOException {
        String line = null;
        while ((line = in.readLine()) != null) {
            if (line.contains("@PACKAGE@")) {
                line = line.replace("@PACKAGE@", packageName);
            }
            sb.append(line).append('\n');
        }
    }
}
