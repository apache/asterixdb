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
package org.apache.hyracks.control.cc.executor;

public final class Runnability {
    private final Tag tag;

    private final int priority;

    public Runnability(Tag tag, int priority) {
        this.tag = tag;
        this.priority = priority;
    }

    public Tag getTag() {
        return tag;
    }

    public int getPriority() {
        return priority;
    }

    public enum Tag {
        COMPLETED,
        NOT_RUNNABLE,
        RUNNABLE,
        RUNNING,
    }

    public static Runnability getWorstCase(Runnability r1, Runnability r2) {
        switch (r1.tag) {
            case COMPLETED:
                switch (r2.tag) {
                    case COMPLETED:
                    case NOT_RUNNABLE:
                    case RUNNABLE:
                    case RUNNING:
                        return r2;
                }
                break;

            case NOT_RUNNABLE:
                switch (r2.tag) {
                    case COMPLETED:
                    case NOT_RUNNABLE:
                    case RUNNABLE:
                    case RUNNING:
                        return r1;
                }
                break;

            case RUNNABLE:
                switch (r2.tag) {
                    case COMPLETED:
                        return r1;

                    case RUNNING:
                        return r1.priority > 0 ? r1 : new Runnability(Tag.RUNNABLE, 1);

                    case NOT_RUNNABLE:
                        return r2;

                    case RUNNABLE:
                        return r1.priority > r2.priority ? r1 : r2;
                }
                break;

            case RUNNING:
                switch (r2.tag) {
                    case COMPLETED:
                    case RUNNING:
                        return r1;

                    case NOT_RUNNABLE:
                        return r2;

                    case RUNNABLE:
                        return r2.priority > 0 ? r2 : new Runnability(Tag.RUNNABLE, 1);
                }
                break;
        }
        throw new IllegalArgumentException("Could not aggregate: " + r1 + " and " + r2);
    }

    @Override
    public String toString() {
        return "{" + tag + ", " + priority + "}";
    }
}
