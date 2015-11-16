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

import java.util.Map;
import java.util.Properties;

public class AsterixBuildProperties extends AbstractAsterixProperties {

    public AsterixBuildProperties(AsterixPropertiesAccessor accessor) {
        super(accessor);
    }

    public String getUserEmail() {
        return accessor.getProperty("git.build.user.email", "", PropertyInterpreters.getStringPropertyInterpreter());
    }

    public String getBuildHost() {
        return accessor.getProperty("git.build.host", "", PropertyInterpreters.getStringPropertyInterpreter());
    }

    public String getDirty() {
        return accessor.getProperty("git.dirty", "", PropertyInterpreters.getStringPropertyInterpreter());
    }

    public String getRemoteOriginUrl() {
        return accessor.getProperty("git.remote.origin.url", "", PropertyInterpreters.getStringPropertyInterpreter());
    }

    public String getClosestTagName() {
        return accessor.getProperty("git.closest.tag.name", "", PropertyInterpreters.getStringPropertyInterpreter());
    }

    public String getCommitIdDescribeShort() {
        return accessor.getProperty("git.commit.id.describe-short", "", PropertyInterpreters.getStringPropertyInterpreter());
    }

    public String getCommitUserEmail() {
        return accessor.getProperty("git.commit.user.email", "", PropertyInterpreters.getStringPropertyInterpreter());
    }

    public String getCommitTime() {
        return accessor.getProperty("git.commit.time", "", PropertyInterpreters.getStringPropertyInterpreter());
    }

    public String getCommitMessage() {
        return accessor.getProperty("git.commit.message.full", "", PropertyInterpreters.getStringPropertyInterpreter());
    }

    public String getBuildVersion() {
        return accessor.getProperty("git.build.version", "", PropertyInterpreters.getStringPropertyInterpreter());
    }

    public String getCommitMessageShort() {
        return accessor.getProperty("git.commit.message.short", "", PropertyInterpreters.getStringPropertyInterpreter());
    }

    public String getShortCommitId() {
        return accessor.getProperty("git.commit.id.abbrev", "", PropertyInterpreters.getStringPropertyInterpreter());
    }

    public String getBranch() {
        return accessor.getProperty("git.branch", "", PropertyInterpreters.getStringPropertyInterpreter());
    }

    public String getBuildUserName() {
        return accessor.getProperty("git.build.user.name", "", PropertyInterpreters.getStringPropertyInterpreter());
    }

    public String getClosestTagCommitCount() {
        return accessor.getProperty("git.closest.tag.commit.count", "", PropertyInterpreters.getStringPropertyInterpreter());
    }

    public String getCommitIdDescribe() {
        return accessor.getProperty("git.commit.id.describe", "", PropertyInterpreters.getStringPropertyInterpreter());
    }

    public String getCommitId() {
        return accessor.getProperty("git.commit.id", "", PropertyInterpreters.getStringPropertyInterpreter());
    }

    public String getTags() {
        return accessor.getProperty("git.tags", "", PropertyInterpreters.getStringPropertyInterpreter());
    }

    public String getBuildTime() {
        return accessor.getProperty("git.build.time", "", PropertyInterpreters.getStringPropertyInterpreter());
    }

    public String getCommitUserName() {
        return accessor.getProperty("git.commit.user.name", "", PropertyInterpreters.getStringPropertyInterpreter());
    }
    public Map<String, String> getAllProps(){
        return accessor.getBuildProperties();
    }

}
