/*
 * Copyright 2009-2012 by The Regents of the University of California
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
package edu.uci.ics.asterix.common.api;

import edu.uci.ics.asterix.common.context.AsterixIndexRegistryProvider;
import edu.uci.ics.asterix.common.context.AsterixStorageManagerInterface;
import edu.uci.ics.asterix.common.dataflow.IAsterixApplicationContextInfo;
import edu.uci.ics.hyracks.api.application.ICCApplicationContext;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

/*
 * Acts as an holder class for IndexRegistryProvider, AsterixStorageManager
 * instances that are accessed from the NCs. In addition an instance of ICCApplicationContext 
 * is stored for access by the CC.
 */
public class AsterixAppContextInfoImpl implements IAsterixApplicationContextInfo {

    private static AsterixAppContextInfoImpl INSTANCE;

    private final ICCApplicationContext appCtx;

    public static void initialize(ICCApplicationContext ccAppCtx) {
        if (INSTANCE == null) {
            INSTANCE = new AsterixAppContextInfoImpl(ccAppCtx);
        }
    }

    private AsterixAppContextInfoImpl(ICCApplicationContext ccAppCtx) {
        this.appCtx = ccAppCtx;
    }

    public static IAsterixApplicationContextInfo getInstance() {
        return INSTANCE;
    }

    @Override
    public IIndexRegistryProvider<IIndex> getIndexRegistryProvider() {
        return AsterixIndexRegistryProvider.INSTANCE;
    }

    @Override
    public IStorageManagerInterface getStorageManagerInterface() {
        return AsterixStorageManagerInterface.INSTANCE;
    }

    @Override
    public ICCApplicationContext getCCApplicationContext() {
        return appCtx;
    }

}
