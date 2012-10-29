/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.control.cc.work;

import java.io.IOException;
import java.util.Map;

import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.application.CCApplicationContext;
import edu.uci.ics.hyracks.control.common.application.ApplicationStatus;
import edu.uci.ics.hyracks.control.common.work.AbstractWork;
import edu.uci.ics.hyracks.control.common.work.IResultCallback;

public class ApplicationCreateWork extends AbstractWork {
    private final ClusterControllerService ccs;
    private final String appName;
    private IResultCallback<Object> callback;

    public ApplicationCreateWork(ClusterControllerService ccs, String appName, IResultCallback<Object> callback) {
        this.ccs = ccs;
        this.appName = appName;
        this.callback = callback;
    }

    @Override
    public void run() {
        try {
            Map<String, CCApplicationContext> applications = ccs.getApplicationMap();
            if (applications.containsKey(appName)) {
                callback.setException(new HyracksException("Duplicate application with name: " + appName
                        + " being created."));
                return;
            }
            CCApplicationContext appCtx;
            try {
                appCtx = new CCApplicationContext(ccs.getServerContext(), ccs.getCCContext(), appName);
            } catch (IOException e) {
                callback.setException(e);
                return;
            }
            appCtx.setStatus(ApplicationStatus.CREATED);
            applications.put(appName, appCtx);
            callback.setValue(null);
        } catch (Exception e) {
            callback.setException(e);
        }
    }
}