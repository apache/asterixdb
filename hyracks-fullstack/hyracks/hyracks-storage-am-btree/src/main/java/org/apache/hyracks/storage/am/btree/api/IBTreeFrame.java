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

package org.apache.hyracks.storage.am.btree.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.BTreeOpContext.PageValidationInfo;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;

public interface IBTreeFrame extends ITreeIndexFrame {
    public int findInsertTupleIndex(ITupleReference tuple) throws HyracksDataException;

    public int findDeleteTupleIndex(ITupleReference tuple) throws HyracksDataException;

    public void insertSorted(ITupleReference tuple) throws HyracksDataException;

    public void setSmFlag(boolean smFlag);

    public boolean getSmFlag();

    public void setLargeFlag(boolean largePage);

    public boolean getLargeFlag();

    public void validate(PageValidationInfo pvi) throws HyracksDataException;
}
