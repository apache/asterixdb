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
package org.apache.asterix.common.channels;

import java.io.Serializable;

/**
 * A unique identifier for a channel.
 */
public class ChannelId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String dataverse;
    private final String channelName;

    public ChannelId(String dataverse, String channelName) {
        this.dataverse = dataverse;
        this.channelName = channelName;
    }

    public String getDataverse() {
        return dataverse;
    }

    public String getChannelName() {
        return channelName;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof ChannelId)) {
            return false;
        }
        if (this == o || ((ChannelId) o).getChannelName().equals(channelName)
                && ((ChannelId) o).getDataverse().equals(dataverse)) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public String toString() {
        return dataverse + "." + channelName;
    }
}