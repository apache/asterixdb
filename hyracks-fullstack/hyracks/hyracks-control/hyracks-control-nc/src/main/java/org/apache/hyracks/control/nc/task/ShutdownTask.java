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

package org.apache.hyracks.control.nc.task;

import static org.apache.hyracks.util.ExitUtil.EC_NORMAL_TERMINATION;
import static org.apache.hyracks.util.ExitUtil.EC_TERMINATE_NC_SERVICE_DIRECTIVE;

import org.apache.hyracks.util.ExitUtil;

public class ShutdownTask implements Runnable {
    private final boolean terminateNCService;

    public ShutdownTask(boolean terminateNCService) {
        this.terminateNCService = terminateNCService;
    }

    @Override
    public void run() {
        ExitUtil.exit(terminateNCService ? EC_TERMINATE_NC_SERVICE_DIRECTIVE : EC_NORMAL_TERMINATION);
    }

}
