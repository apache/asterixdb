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
package org.apache.hyracks.cloud.io;

import static org.apache.hyracks.util.annotations.AiProvenance.Agent.CLAUDE_SONNET_4_6;
import static org.apache.hyracks.util.annotations.AiProvenance.ContributionKind.GENERATED;
import static org.apache.hyracks.util.annotations.AiProvenance.Tool.GITHUB_COPILOT;

import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Controls the AWS SDK v2 checksum behavior for S3 clients.
 * Introduced in SDK 2.30.0 where the default changed to {@link #WHEN_SUPPORTED},
 * which is not supported by all S3-compatible storage solutions.
 */
@AiProvenance(agent = CLAUDE_SONNET_4_6, tool = GITHUB_COPILOT, contributionKind = GENERATED)
public enum S3ChecksumBehavior {
    /** Calculate/validate checksums only when required by the operation. Safe for S3-compatible endpoints. */
    WHEN_REQUIRED,
    /** Calculate/validate checksums whenever supported — the SDK default since 2.30.0. */
    WHEN_SUPPORTED,
    /** Leave the SDK defaults untouched. Appropriate for native AWS S3. */
    SDK_DEFAULT;

    public String stringValue() {
        return name().toLowerCase();
    }

    /** Parses the config string (case-insensitive). Returns {@code SDK_DEFAULT} if the input is {@code null}. */
    public static S3ChecksumBehavior fromString(String s) {
        if (s == null) {
            return SDK_DEFAULT;
        }
        for (S3ChecksumBehavior b : values()) {
            if (b.name().equalsIgnoreCase(s)) {
                return b;
            }
        }
        throw new IllegalArgumentException("Unrecognized S3 checksum behavior: '" + s
                + "'. Valid values: when_required, when_supported, sdk_default");
    }

}
