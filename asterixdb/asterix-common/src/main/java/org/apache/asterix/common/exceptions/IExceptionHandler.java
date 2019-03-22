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
package org.apache.asterix.common.exceptions;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Handles an exception encountered during processing of a data frame.
 * In the case when the exception is of type {@code FrameDataException}, the causing
 * tuple is logged and a new frame with tuple after the exception-generating tuple
 * is returned. This functionality is used during feed ingestion to bypass an exception
 * generating tuple and thus avoid the data flow from terminating
 */
public interface IExceptionHandler {

    /**
     * @param e
     *            the exception that needs to be handled
     * @param frame
     *            the frame that was being processed when exception occurred
     * @return returns a new frame with tuples after the exception generating tuple
     * @throws HyracksDataException
     */
    public ByteBuffer handle(HyracksDataException e, ByteBuffer frame);
}
