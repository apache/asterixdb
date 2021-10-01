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

package org.apache.asterix.jdbc.core;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class ADBProductVersion {

    public static final String ASTERIXDB = "Apache AsterixDB";

    private static final Pattern DATABASE_VERSION_PATTERN =
            Pattern.compile("(?<name>[^/]+)(?:/(?<ver>(?:(?<vermj>\\d+)(?:\\.(?<vermn>\\d+))?)?.*))?");

    final String productName;

    final String productVersion;

    final int majorVersion;

    final int minorVersion;

    private ADBProductVersion(String productName, String productVersion, int majorVersion, int minorVersion) {
        this.productName = productName != null ? productName : ASTERIXDB;
        this.productVersion = productVersion != null ? productVersion : majorVersion + "." + minorVersion;
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
    }

    static ADBProductVersion parseDriverVersion(Package driverPackage) {
        int majorVersion = 0, minorVersion = 0;
        String productName = driverPackage.getImplementationTitle();
        if (productName == null) {
            productName = ASTERIXDB;
        }
        String productVersion = driverPackage.getImplementationVersion();
        if (productVersion != null) {
            String[] v = productVersion.split("\\.");
            try {
                majorVersion = Integer.parseInt(v[0]);
                if (v.length > 1) {
                    minorVersion = Integer.parseInt(v[1]);
                }
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        return new ADBProductVersion(productName, productVersion, majorVersion, minorVersion);
    }

    static ADBProductVersion parseDatabaseVersion(String serverVersion) {
        String dbProductName = null;
        String dbProductVersion = null;
        int dbMajorVersion = 0;
        int dbMinorVersion = 0;
        if (serverVersion != null) {
            Matcher m = DATABASE_VERSION_PATTERN.matcher(serverVersion);
            if (m.matches()) {
                dbProductName = m.group("name");
                dbProductVersion = m.group("ver");
                String vermj = m.group("vermj");
                String vermn = m.group("vermn");
                if (vermj != null) {
                    try {
                        dbMajorVersion = Integer.parseInt(vermj);
                    } catch (NumberFormatException e) {
                        // ignore (overflow)
                    }
                }
                if (vermn != null) {
                    try {
                        dbMinorVersion = Integer.parseInt(vermn);
                    } catch (NumberFormatException e) {
                        // ignore (overflow)
                    }
                }
            }
        }
        return new ADBProductVersion(dbProductName, dbProductVersion, dbMajorVersion, dbMinorVersion);
    }
}
