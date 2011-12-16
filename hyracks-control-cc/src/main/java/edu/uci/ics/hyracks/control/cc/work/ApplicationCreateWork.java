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
import edu.uci.ics.hyracks.control.common.work.AbstractWork;
import edu.uci.ics.hyracks.control.common.work.FutureValue;

public class ApplicationCreateWork extends AbstractWork {
    private final ClusterControllerService ccs;
    private final String appName;
    private FutureValue<Object> fv;

    public ApplicationCreateWork(ClusterControllerService ccs, String appName, FutureValue<Object> fv) {
        this.ccs = ccs;
        this.appName = appName;
        this.fv = fv;
    }

    @Override
    public void run() {
        Map<String, CCApplicationContext> applications = ccs.getApplicationMap();
        if (applications.containsKey(appName)) {
            fv.setException(new HyracksException("Duplicate application with name: " + appName + " being created."));
        }
        CCApplicationContext appCtx;
        try {
            appCtx = new CCApplicationContext(ccs.getServerContext(), ccs.getCCContext(), appName);
        } catch (IOException e) {
            fv.setException(e);
            return;
        }
        applications.put(appName, appCtx);
        fv.setValue(null);
    }
}