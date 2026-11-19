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
package org.apache.asterix.api.http.server;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class CredentialFileUtil {

    private CredentialFileUtil() {
    }

    /**
     * Reads a passwd file of {@code user:bcrypt-hash} lines.
     *
     * @param credPath
     *            path of the passwd file
     * @param required
     *            whether the file must exist; when {@code false} a missing file yields an empty map
     * @return the stored credentials, keyed by user name
     * @throws IllegalStateException
     *             if the file is required but missing, or if it exists but cannot be read or is malformed
     */
    public static Map<String, String> load(String credPath, boolean required) {
        Map<String, String> storedCredentials = new HashMap<>();
        File credentialFile = credPath != null ? new File(credPath) : null;
        if (credentialFile == null || !credentialFile.isFile()) {
            if (required) {
                throw new IllegalStateException(
                        "credential file " + (credentialFile != null ? credentialFile.getAbsolutePath() : null)
                                + " does not exist; create it or disable authentication");
            }
            return storedCredentials;
        }
        try (CSVParser p =
                CSVParser.parse(credentialFile, Charset.defaultCharset(), CSVFormat.DEFAULT.withDelimiter(':'))) {
            for (CSVRecord r : p.getRecords()) {
                if (r.size() != 2) {
                    throw new IllegalStateException("malformed credential file " + credentialFile.getAbsolutePath()
                            + ": line " + r.getRecordNumber() + " must have exactly two fields");
                }
                storedCredentials.put(r.get(0), r.get(1));
            }
        } catch (IOException e) {
            throw new IllegalStateException("failed to read credential file " + credentialFile.getAbsolutePath(), e);
        }
        return storedCredentials;
    }
}
