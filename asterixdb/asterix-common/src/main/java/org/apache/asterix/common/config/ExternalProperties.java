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

import static org.apache.hyracks.control.common.config.OptionTypes.LEVEL;
import static org.apache.hyracks.control.common.config.OptionTypes.POSITIVE_INTEGER;
import static org.apache.hyracks.control.common.config.OptionTypes.STRING;
import static org.apache.hyracks.control.common.config.OptionTypes.UNSIGNED_INTEGER;

import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.util.StorageUtil;
import org.apache.logging.log4j.Level;

public class ExternalProperties extends AbstractProperties {

    public enum Option implements IOption {
        WEB_PORT(UNSIGNED_INTEGER, 19001, "The listen port of the legacy query interface"),
        WEB_QUERYINTERFACE_PORT(UNSIGNED_INTEGER, 19006, "The listen port of the query web interface"),
        API_PORT(UNSIGNED_INTEGER, 19002, "The listen port of the API server"),
        ACTIVE_PORT(UNSIGNED_INTEGER, 19003, "The listen port of the active server"),
        NC_API_PORT(UNSIGNED_INTEGER, 19004, "The listen port of the node controller API server"),
        LOG_LEVEL(LEVEL, Level.WARN, "The logging level for master and slave processes"),
        MAX_WAIT_ACTIVE_CLUSTER(
                POSITIVE_INTEGER,
                60,
                "The max pending time (in seconds) for cluster startup. After the "
                        + "threshold, if the cluster still is not up and running, it is considered unavailable"),
        CC_JAVA_OPTS(STRING, "-Xmx1024m", "The JVM options passed to the cluster controller process by managix"),
        NC_JAVA_OPTS(STRING, "-Xmx1024m", "The JVM options passed to the node controller process(es) by managix"),
        MAX_WEB_REQUEST_SIZE(
                UNSIGNED_INTEGER,
                StorageUtil.getIntSizeInBytes(50, StorageUtil.StorageUnit.MEGABYTE),
                "The maximum accepted web request size in bytes"),
        REQUESTS_ARCHIVE_SIZE(UNSIGNED_INTEGER, 50, "The maximum number of archived requests to maintain");

        private final IOptionType type;
        private final Object defaultValue;
        private final String description;

        Option(IOptionType type, Object defaultValue, String description) {
            this.type = type;
            this.defaultValue = defaultValue;
            this.description = description;
        }

        @Override
        public Section section() {
            switch (this) {
                case WEB_PORT:
                case WEB_QUERYINTERFACE_PORT:
                case API_PORT:
                case ACTIVE_PORT:
                case REQUESTS_ARCHIVE_SIZE:
                    return Section.CC;
                case NC_API_PORT:
                    return Section.NC;
                case LOG_LEVEL:
                case MAX_WAIT_ACTIVE_CLUSTER:
                case MAX_WEB_REQUEST_SIZE:
                    return Section.COMMON;
                case CC_JAVA_OPTS:
                case NC_JAVA_OPTS:
                    return Section.VIRTUAL;
                default:
                    throw new IllegalStateException("NYI: " + this);
            }
        }

        @Override
        public String description() {
            return description;
        }

        @Override
        public IOptionType type() {
            return type;
        }

        @Override
        public Object defaultValue() {
            return defaultValue;
        }
    }

    public ExternalProperties(PropertiesAccessor accessor) {
        super(accessor);
    }

    public int getWebInterfacePort() {
        return accessor.getInt(Option.WEB_PORT);
    }

    public int getQueryWebInterfacePort() {
        return accessor.getInt(Option.WEB_QUERYINTERFACE_PORT);
    }

    public int getAPIServerPort() {
        return accessor.getInt(Option.API_PORT);
    }

    public int getActiveServerPort() {
        return accessor.getInt(Option.ACTIVE_PORT);
    }

    public Level getLogLevel() {
        return accessor.getLoggingLevel(Option.LOG_LEVEL);
    }

    public int getMaxWaitClusterActive() {
        return accessor.getInt(Option.MAX_WAIT_ACTIVE_CLUSTER);
    }

    public String getNCJavaParams() {
        return accessor.getString(Option.NC_JAVA_OPTS);
    }

    public String getCCJavaParams() {
        return accessor.getString(Option.CC_JAVA_OPTS);
    }

    public int getNcApiPort() {
        return accessor.getInt(Option.NC_API_PORT);
    }

    public int getMaxWebRequestSize() {
        return accessor.getInt(Option.MAX_WEB_REQUEST_SIZE);
    }

    public int getRequestsArchiveSize() {
        return accessor.getInt(Option.REQUESTS_ARCHIVE_SIZE);
    }
}
