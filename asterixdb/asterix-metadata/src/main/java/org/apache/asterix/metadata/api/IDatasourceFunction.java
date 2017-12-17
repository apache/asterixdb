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
package org.apache.asterix.metadata.api;

import java.io.Serializable;

import org.apache.asterix.external.api.IRecordReader;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * A function that is also a datasource
 */
public interface IDatasourceFunction extends Serializable {

    /**
     * @return the locations on which the function is to be run
     */
    AlgebricksAbsolutePartitionConstraint getPartitionConstraint();

    /**
     * The function record reader
     *
     * @param ctx
     * @param partition
     * @return
     * @throws HyracksDataException
     */
    IRecordReader<char[]> createRecordReader(IHyracksTaskContext ctx, int partition) throws HyracksDataException;

}
