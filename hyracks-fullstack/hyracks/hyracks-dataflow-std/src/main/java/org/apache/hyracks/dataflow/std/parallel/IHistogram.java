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

package org.apache.hyracks.dataflow.std.parallel;

import java.util.List;
import java.util.Map.Entry;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * @author michael
 */
public interface IHistogram<E> {

    public enum FieldType {
        SHORT,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        UTF8
    }

    public void initialize();

    public FieldType getType();

    public void merge(IHistogram<E> ba) throws HyracksDataException;

    public void addItem(E item) throws HyracksDataException;

    public void countItem(E item) throws HyracksDataException;

    public void countReset() throws HyracksDataException;

    public int getCurrent() throws HyracksDataException;

    public List<Entry<E, Integer>> generate(boolean isGlobal) throws HyracksDataException;
}
