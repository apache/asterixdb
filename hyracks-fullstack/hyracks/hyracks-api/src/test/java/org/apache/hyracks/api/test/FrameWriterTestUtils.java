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
package org.apache.hyracks.api.test;

import java.util.Collection;
import java.util.Collections;

public class FrameWriterTestUtils {
    public static final String EXCEPTION_MESSAGE = "IFrameWriter Exception in the call to the method ";
    public static final String ERROR_MESSAGE = "IFrameWriter Error in the call to the method ";

    public enum FrameWriterOperation {
        Open,
        NextFrame,
        Fail,
        Flush,
        Close
    }

    public static TestFrameWriter create() {
        return create(Collections.emptyList(), Collections.emptyList(), false);
    }

    public static TestFrameWriter create(Collection<FrameWriterOperation> exceptionThrowingOperations,
            Collection<FrameWriterOperation> errorThrowingOperations, boolean deepCopyInputFrames) {
        CountAnswer openAnswer =
                createAnswer(FrameWriterOperation.Open, exceptionThrowingOperations, errorThrowingOperations);
        CountAnswer nextAnswer =
                createAnswer(FrameWriterOperation.NextFrame, exceptionThrowingOperations, errorThrowingOperations);
        CountAnswer flushAnswer =
                createAnswer(FrameWriterOperation.Flush, exceptionThrowingOperations, errorThrowingOperations);
        CountAnswer failAnswer =
                createAnswer(FrameWriterOperation.Fail, exceptionThrowingOperations, errorThrowingOperations);
        CountAnswer closeAnswer =
                createAnswer(FrameWriterOperation.Close, exceptionThrowingOperations, errorThrowingOperations);
        return new TestFrameWriter(openAnswer, nextAnswer, flushAnswer, failAnswer, closeAnswer, deepCopyInputFrames);
    }

    public static CountAnswer createAnswer(FrameWriterOperation operation,
            Collection<FrameWriterOperation> exceptionThrowingOperations,
            Collection<FrameWriterOperation> errorThrowingOperations) {
        if (exceptionThrowingOperations.contains(operation)) {
            return new CountAndThrowException(EXCEPTION_MESSAGE + operation.toString());
        } else if (exceptionThrowingOperations.contains(operation)) {
            return new CountAndThrowError(ERROR_MESSAGE + operation.toString());
        } else {
            return new CountAnswer();
        }
    }

    public static TestControlledFrameWriter create(int initialFrameSize, boolean deepCopyInputFrames) {
        return new TestControlledFrameWriter(initialFrameSize, deepCopyInputFrames);
    }
}
