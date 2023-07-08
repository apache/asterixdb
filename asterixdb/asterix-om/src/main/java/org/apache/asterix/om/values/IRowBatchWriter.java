///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//package org.apache.asterix.om.values;
//
//import java.nio.ByteBuffer;
//import java.util.PriorityQueue;
//
//import org.apache.hyracks.api.exceptions.HyracksDataException;
//
//public interface IRowBatchWriter {
//    void setPageZeroBuffer(ByteBuffer pageZeroBuffer, int numberOfColumns, int numberOfPrimaryKeys);
//
//    /**
//     * Writes the primary keys' values to Page0
//     *
//     * @param primaryKeyWriters primary keys' writers
//     * @return the allocated space for the primary keys' writers
//     */
//    int writePrimaryKeyColumns(IRowValuesWriter[] primaryKeyWriters) throws HyracksDataException;
//
//    /**
//     * Writes the non-key values to multiple pages
//     *
//     * @param nonKeysColumnWriters non-key values' writers
//     * @return the allocated space for the non-key values' writers
//     */
//    int writeColumns(PriorityQueue<IRowValuesWriter> nonKeysColumnWriters) throws HyracksDataException;
//}
