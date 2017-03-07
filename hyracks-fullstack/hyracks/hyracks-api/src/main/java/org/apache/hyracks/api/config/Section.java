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
package org.apache.hyracks.api.config;

public enum Section {
    CC,
    NC,
    COMMON,
    LOCALNC,
    EXTENSION,
    VIRTUAL; // virtual section indicates options which are not accessible from the cmd-line nor ini file

    public static Section parseSectionName(String name) {
        for (Section section : values()) {
            if (section.sectionName().equals(name)) {
                return section;
            }
        }
        return null;
    }

    public String sectionName() {
        return name().toLowerCase();
    }
}
