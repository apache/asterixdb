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

package org.apache.hyracks.storage.am.lsm.invertedindex.tuples;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class TokenKeyPairTuple implements ITupleReference {

    private ITupleReference tokenTuple;
    private ITupleReference keyTuple;

    private final int tokenFieldCount;
    private final int keyFieldCount;

    private boolean newToken;

    public TokenKeyPairTuple(int tokenFieldCount, int keyFieldCount) {
        this.tokenFieldCount = tokenFieldCount;
        this.keyFieldCount = keyFieldCount;

    }

    public void setTokenTuple(ITupleReference token) {
        this.tokenTuple = token;
        this.keyTuple = null;
    }

    public void setKeyTuple(ITupleReference key) {
        newToken = this.keyTuple == null;
        this.keyTuple = key;
    }

    public ITupleReference getTokenTuple() {
        return tokenTuple;
    }

    public ITupleReference getKeyTuple() {
        return keyTuple;
    }

    @Override
    public int getFieldCount() {
        return tokenFieldCount + keyFieldCount;
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        ITupleReference tuple = getTuple(fIdx);
        int fieldIndex = getFieldIndex(fIdx);
        return tuple.getFieldData(fieldIndex);
    }

    @Override
    public int getFieldStart(int fIdx) {
        ITupleReference tuple = getTuple(fIdx);
        int fieldIndex = getFieldIndex(fIdx);
        return tuple.getFieldStart(fieldIndex);
    }

    @Override
    public int getFieldLength(int fIdx) {
        ITupleReference tuple = getTuple(fIdx);
        int fieldIndex = getFieldIndex(fIdx);
        return tuple.getFieldLength(fieldIndex);
    }

    private ITupleReference getTuple(int fIdx) {
        return fIdx < tokenFieldCount ? tokenTuple : keyTuple;
    }

    private int getFieldIndex(int fIdx) {
        return fIdx < tokenFieldCount ? fIdx : fIdx - tokenFieldCount;
    }

    public boolean isNewToken() {
        return newToken;
    }
}
