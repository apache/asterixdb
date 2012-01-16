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
package edu.uci.ics.hyracks.control.nc.work;

import java.util.Map;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.control.common.application.ApplicationContext;
import edu.uci.ics.hyracks.control.common.work.FutureValue;
import edu.uci.ics.hyracks.control.common.work.SynchronizableWork;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.control.nc.application.NCApplicationContext;

public class DestroyApplicationWork extends SynchronizableWork {
    private static final Logger LOGGER = Logger.getLogger(DestroyApplicationWork.class.getName());

    private final NodeControllerService ncs;

    private final String appName;

    private FutureValue<Object> fv;

    public DestroyApplicationWork(NodeControllerService ncs, String appName, FutureValue<Object> fv) {
        this.ncs = ncs;
        this.appName = appName;
        this.fv = fv;
    }

    @Override
    protected void doRun() throws Exception {
        try {
            Map<String, NCApplicationContext> applications = ncs.getApplications();
            ApplicationContext appCtx = applications.remove(appName);
            if (appCtx != null) {
                appCtx.deinitialize();
            }
            fv.setValue(null);
        } catch (Exception e) {
            fv.setException(e);
        }
    }
}