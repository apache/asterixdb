/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.asterix.metadata.feeds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handle (de)registration of feeds for delivery of control messages.
 */
public class FeedManager implements IFeedManager {

    public static FeedManager INSTANCE = new FeedManager();

    private FeedManager() {

    }

    private Map<FeedId, List<AdapterRuntimeManager>> activeFeedRuntimeManagers = new HashMap<FeedId, List<AdapterRuntimeManager>>();

    @Override
    public synchronized void registerFeedRuntime(AdapterRuntimeManager adapterRuntimeMgr) {
        List<AdapterRuntimeManager> adpaterRuntimeMgrs = activeFeedRuntimeManagers.get(adapterRuntimeMgr.getFeedId());
        if (adpaterRuntimeMgrs == null) {
            adpaterRuntimeMgrs = new ArrayList<AdapterRuntimeManager>();
            activeFeedRuntimeManagers.put(adapterRuntimeMgr.getFeedId(), adpaterRuntimeMgrs);
        }
        adpaterRuntimeMgrs.add(adapterRuntimeMgr);
    }

    @Override
    public synchronized void deRegisterFeedRuntime(AdapterRuntimeManager adapterRuntimeMgr) {
        List<AdapterRuntimeManager> adapterRuntimeMgrs = activeFeedRuntimeManagers.get(adapterRuntimeMgr.getFeedId());
        if (adapterRuntimeMgrs != null && adapterRuntimeMgrs.contains(adapterRuntimeMgr)) {
            adapterRuntimeMgrs.remove(adapterRuntimeMgr);
        }
    }

    @Override
    public synchronized AdapterRuntimeManager getFeedRuntimeManager(FeedId feedId, int partition) {
        List<AdapterRuntimeManager> adapterRuntimeMgrs = activeFeedRuntimeManagers.get(feedId);
        if (adapterRuntimeMgrs != null) {
            for (AdapterRuntimeManager mgr : adapterRuntimeMgrs) {
                if (mgr.getPartition() == partition) {
                    return mgr;
                }
            }
        }
        return null;
    }

    public List<AdapterRuntimeManager> getFeedRuntimeManagers(FeedId feedId) {
        return activeFeedRuntimeManagers.get(feedId);
    }

}
