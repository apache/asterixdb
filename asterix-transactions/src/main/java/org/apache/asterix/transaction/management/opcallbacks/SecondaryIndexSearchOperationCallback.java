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

package org.apache.asterix.transaction.management.opcallbacks;

import org.apache.asterix.common.transactions.AbstractOperationCallback;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallback;

/**
 * Secondary index searches perform no locking at all.
 */
public class SecondaryIndexSearchOperationCallback extends AbstractOperationCallback implements
        ISearchOperationCallback {

    public SecondaryIndexSearchOperationCallback() {
        super(-1, null, null, null);
    }

    @Override
    public boolean proceed(ITupleReference tuple) throws HyracksDataException {
        return true;
    }

    @Override
    public void reconcile(ITupleReference tuple) throws HyracksDataException {
        // Do nothing.
    }

    @Override
    public void cancel(ITupleReference tuple) throws HyracksDataException {
        // Do nothing.
    }

    @Override
    public void complete(ITupleReference tuple) throws HyracksDataException {
        // Do nothing.
    }

}
