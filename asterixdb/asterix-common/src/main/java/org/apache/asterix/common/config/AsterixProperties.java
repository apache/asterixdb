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
package org.apache.asterix.common.config;

import java.io.File;

public class AsterixProperties {
    //---------------------------- Directories ---------------------------//
    private static final String VAR = File.separator + "var";
    private static final String LIB = VAR + File.separator + "lib";
    private static final String ASTERIXDB = LIB + File.separator + "asterixdb";
    //----------------------------- Sections -----------------------------//
    public static final String SECTION_ASTERIX = "asterix";
    public static final String SECTION_PREFIX_EXTENSION = "extension/";
    public static final String SECTION_CC = "cc";
    public static final String SECTION_PREFIX_NC = "nc/";
    //---------------------------- Properties ---=------------------------//
    public static final String PROPERTY_CLUSTER_ADDRESS = "cluster.address";
    public static final String PROPERTY_INSTANCE_NAME = "instance";
    public static final String DEFAULT_INSTANCE_NAME = "DEFAULT_INSTANCE";
    public static final String PROPERTY_METADATA_NODE = "metadata.node";
    public static final String PROPERTY_COREDUMP_DIR = "coredumpdir";
    public static final String DEFAULT_COREDUMP_DIR = String.join(File.separator, ASTERIXDB, "coredump");
    public static final String PROPERTY_TXN_LOG_DIR = "txnlogdir";
    public static final String DEFAULT_TXN_LOG_DIR = String.join(File.separator, ASTERIXDB, "txn-log");
    public static final String PROPERTY_IO_DEV = "iodevices";
    public static final String DEFAULT_IO_DEV = String.join(File.separator, ASTERIXDB, "iodevice");
    public static final String PROPERTY_STORAGE_DIR = "storagedir";
    public static final String DEFAULT_STORAGE_DIR = "storage";
    public static final String PROPERTY_CLASS = "class";

    private AsterixProperties() {
    }

    public static final String getSectionId(String prefix, String section) {
        return section.substring(prefix.length());
    }
}
