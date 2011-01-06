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

package edu.uci.ics.hyracks.examples.btree.helper;

import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.application.INCBootstrap;

public class NCBootstrap implements INCBootstrap {
    private static final Logger LOGGER = Logger.getLogger(NCBootstrap.class.getName());

    private INCApplicationContext appCtx;

    @Override
    public void start() throws Exception {
        LOGGER.info("Starting NC Bootstrap");
        RuntimeContext rCtx = new RuntimeContext(appCtx);
        appCtx.setApplicationObject(rCtx);
        LOGGER.info("Initialized RuntimeContext: " + rCtx);
    }

    @Override
    public void stop() throws Exception {
        LOGGER.info("Stopping Asterix NC Bootstrap");
        RuntimeContext rCtx = (RuntimeContext) appCtx.getApplicationObject();
        rCtx.close();
    }

    @Override
    public void setApplicationContext(INCApplicationContext appCtx) {
        this.appCtx = appCtx;
    }
}