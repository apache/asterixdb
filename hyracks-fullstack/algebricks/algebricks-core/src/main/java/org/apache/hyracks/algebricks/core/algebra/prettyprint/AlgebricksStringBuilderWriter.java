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
package org.apache.hyracks.algebricks.core.algebra.prettyprint;

import java.io.IOException;

import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.hyracks.util.annotations.NotThreadSafe;

/**
 * String writer based on string builder to provide un-synchronized writer. It overrides append methods to allow
 * chaining to the same string-builder-based writer to avoid throwing {@link IOException}
 */
@NotThreadSafe
public class AlgebricksStringBuilderWriter extends StringBuilderWriter {

    public AlgebricksStringBuilderWriter() {
        super();
    }

    public AlgebricksStringBuilderWriter(final int capacity) {
        super(capacity);
    }

    @Override
    public AlgebricksStringBuilderWriter append(final char value) {
        super.append(value);
        return this;
    }

    @Override
    public AlgebricksStringBuilderWriter append(final CharSequence value) {
        super.append(value);
        return this;
    }

    @Override
    public AlgebricksStringBuilderWriter append(final CharSequence value, final int start, final int end) {
        super.append(value, start, end);
        return this;
    }
}
