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
package org.apache.asterix.channels;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.channels.ChannelId;
import org.apache.asterix.feeds.ActiveObjectInfo;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

public class ChannelInfo extends ActiveObjectInfo {
    public ChannelId channelId;
    public List<String> locations = new ArrayList<String>();

    public ChannelInfo(ChannelId channelId, JobSpecification jobSpec, JobId jobId) {
        super(jobSpec, jobId);
        this.channelId = channelId;
    }

    @Override
    public String toString() {
        return "Channel" + "[" + channelId + "]";
    }
}
