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
package edu.uci.ics.asterix.common.feeds.api;

import edu.uci.ics.asterix.common.feeds.FeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.FeedRuntimeInputHandler;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;

public interface IFeedRuntime {

    public enum FeedRuntimeType {
        INTAKE,
        COLLECT,
        COMPUTE_COLLECT,
        COMPUTE,
        STORE,
        OTHER,
        ETS,
        JOIN
    }

    public enum Mode {
        PROCESS,
        SPILL,
        PROCESS_SPILL,
        DISCARD,
        POST_SPILL_DISCARD,
        PROCESS_BACKLOG,
        STALL,
        FAIL,
        END
    }

    /**
     * @return the unique runtime id associated with the feedRuntime
     */
    public FeedRuntimeId getRuntimeId();

    /**
     * @return the frame writer associated with the feed runtime.
     */
    public IFrameWriter getFeedFrameWriter();

    public FeedRuntimeInputHandler getInputHandler();

}
