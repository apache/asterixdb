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
package org.apache.hyracks.maven.license;

import static org.apache.hyracks.maven.license.LicenseUtil.toGav;

import java.util.Properties;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.util.StringUtil;
import org.apache.maven.project.MavenProject;

enum ProjectFlag {
    IGNORE_MISSING_EMBEDDED_LICENSE,
    IGNORE_MISSING_EMBEDDED_NOTICE,
    IGNORE_LICENSE_OVERRIDE,
    IGNORE_NOTICE_OVERRIDE,
    ALTERNATE_LICENSE_FILE,
    ALTERNATE_NOTICE_FILE;

    String propName() {
        return "license." + StringUtil.toCamelCase(name());
    }

    void visit(MavenProject depObj, Properties properties, LicenseMojo licenseMojo) {
        String value = properties.getProperty(propName());
        if (value == null) {
            return;
        }
        switch (this) {
            case IGNORE_MISSING_EMBEDDED_LICENSE:
            case IGNORE_MISSING_EMBEDDED_NOTICE:
            case IGNORE_LICENSE_OVERRIDE:
            case IGNORE_NOTICE_OVERRIDE:
                if (Stream.of(StringUtils.split(value, ",")).anyMatch(depObj.getVersion()::equals)) {
                    licenseMojo.getProjectFlags().put(Pair.of(toGav(depObj), this), Boolean.TRUE);
                } else {
                    licenseMojo.getLog().info(propName() + " defined on versions that *do not* match: " + value
                            + " for " + toGav(depObj));
                }
                break;
            case ALTERNATE_LICENSE_FILE:
            case ALTERNATE_NOTICE_FILE:
                for (String spec : StringUtils.split(value, ",")) {
                    String[] specSplit = StringUtils.split(spec, ":");
                    if (specSplit.length != 2) {
                        throw new IllegalArgumentException(spec);
                    }
                    if (specSplit[0].equals(depObj.getVersion())) {
                        licenseMojo.getProjectFlags().put(Pair.of(toGav(depObj), this), specSplit[1]);
                    }
                }
                break;
            default:
                throw new IllegalStateException("NYI: " + this);
        }
    }
}
