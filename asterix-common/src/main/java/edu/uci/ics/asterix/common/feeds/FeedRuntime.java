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
package edu.uci.ics.asterix.common.feeds;

import edu.uci.ics.asterix.common.feeds.api.IFeedOperatorOutputSideHandler;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;

public class FeedRuntime implements IFeedRuntime {

    /** A unique identifier for the runtime **/
    protected final FeedRuntimeId runtimeId;

    /** The output frame writer associated with the runtime **/
    protected IFrameWriter frameWriter;

    /** The pre-processor associated with the runtime **/
    protected FeedRuntimeInputHandler inputHandler;

    public FeedRuntime(FeedRuntimeId runtimeId, FeedRuntimeInputHandler inputHandler, IFrameWriter frameWriter) {
        this.runtimeId = runtimeId;
        this.frameWriter = frameWriter;
        this.inputHandler = inputHandler;
    }

    public void setFrameWriter(IFeedOperatorOutputSideHandler frameWriter) {
        this.frameWriter = frameWriter;
    }

    @Override
    public FeedRuntimeId getRuntimeId() {
        return runtimeId;
    }

    @Override
    public IFrameWriter getFeedFrameWriter() {
        return frameWriter;
    }

    @Override
    public String toString() {
        return runtimeId.toString();
    }

    @Override
    public FeedRuntimeInputHandler getInputHandler() {
        return inputHandler;
    }

    public Mode getMode() {
        return inputHandler != null ? inputHandler.getMode() : Mode.PROCESS;
    }

    public void setMode(Mode mode) {
        this.inputHandler.setMode(mode);
    }

}
