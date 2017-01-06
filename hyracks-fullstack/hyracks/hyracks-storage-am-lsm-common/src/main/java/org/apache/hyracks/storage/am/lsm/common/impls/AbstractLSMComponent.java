/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.storage.am.lsm.common.impls;

import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;

public abstract class AbstractLSMComponent implements ILSMComponent {

    public static final MutableArrayValueReference MARKER_LSN_KEY = new MutableArrayValueReference("Marker"
            .getBytes());

    protected ComponentState state;
    protected int readerCount;
    protected final ILSMComponentFilter filter;
    protected long mostRecentMarkerLSN;
    protected final LongPointable pointable = (LongPointable) LongPointable.FACTORY.createPointable();

    public AbstractLSMComponent(ILSMComponentFilter filter, long mostRecentMarkerLSN) {
        this.filter = filter;
        this.mostRecentMarkerLSN = mostRecentMarkerLSN;
        readerCount = 0;
    }

    public AbstractLSMComponent(ILSMComponentFilter filter) {
        this(filter, -1L);
    }

    public AbstractLSMComponent() {
        this(null);
    }

    @Override
    public ComponentState getState() {
        return state;
    }

    @Override
    public ILSMComponentFilter getLSMComponentFilter() {
        return filter;
    }

    @Override
    public long getMostRecentMarkerLSN() {
        return mostRecentMarkerLSN;
    }

    @Override
    public void setMostRecentMarkerLSN(long lsn) {
        this.mostRecentMarkerLSN = lsn;
    }
}
