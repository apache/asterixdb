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
package org.apache.hyracks.algebricks.core.algebra.expressions;

import java.util.Objects;

public final class BroadcastExpressionAnnotation implements IExpressionAnnotation {

    public enum BroadcastSide {
        LEFT,
        RIGHT;

        public static BroadcastSide getOppositeSide(BroadcastSide side) {
            switch (side) {
                case LEFT:
                    return RIGHT;
                case RIGHT:
                    return LEFT;
                default:
                    throw new IllegalStateException(String.valueOf(side));
            }
        }
    }

    private final BroadcastSide side;

    public BroadcastExpressionAnnotation(BroadcastSide side) {
        this.side = Objects.requireNonNull(side);
    }

    public BroadcastSide getBroadcastSide() {
        return side;
    }
}
