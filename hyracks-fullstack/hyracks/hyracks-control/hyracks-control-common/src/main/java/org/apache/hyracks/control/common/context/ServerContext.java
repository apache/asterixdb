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
package org.apache.hyracks.control.common.context;

import java.io.File;

import org.apache.hyracks.api.application.IServerContext;

public class ServerContext implements IServerContext {

    private final ServerType type;
    private final File baseDir;
    private final File appDir;

    public ServerContext(ServerType type, File baseDir) {
        this.type = type;
        this.baseDir = baseDir;
        this.appDir = new File(baseDir, "applications");
    }

    public ServerType getServerType() {
        return type;
    }

    public File getBaseDir() {
        return baseDir;
    }

    public File getAppDir() {
        return appDir;
    }
}
