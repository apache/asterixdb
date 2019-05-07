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

package org.apache.hyracks.algebricks.core.algebra.properties;

public abstract class LocalMemoryRequirements {

    public abstract int getMinMemoryBudgetInFrames();

    public abstract int getMemoryBudgetInFrames();

    public abstract void setMemoryBudgetInFrames(int value);

    public final long getMemoryBudgetInBytes(long frameSize) {
        return frameSize * getMemoryBudgetInFrames();
    }

    public static LocalMemoryRequirements fixedMemoryBudget(int memBudgetInFrames) {
        if (memBudgetInFrames < 0) {
            throw new IllegalArgumentException(String.valueOf(memBudgetInFrames));
        }
        return memBudgetInFrames == FixedMemoryBudget.ONE_FRAME.memBudgetInFrames ? FixedMemoryBudget.ONE_FRAME
                : new FixedMemoryBudget(memBudgetInFrames);
    }

    private static final class FixedMemoryBudget extends LocalMemoryRequirements {

        private static final FixedMemoryBudget ONE_FRAME = new FixedMemoryBudget(1);

        private final int memBudgetInFrames;

        private FixedMemoryBudget(int memBudgetInFrames) {
            this.memBudgetInFrames = memBudgetInFrames;
        }

        @Override
        public int getMinMemoryBudgetInFrames() {
            return memBudgetInFrames;
        }

        @Override
        public int getMemoryBudgetInFrames() {
            return memBudgetInFrames;
        }

        @Override
        public void setMemoryBudgetInFrames(int value) {
            if (value != memBudgetInFrames) {
                throw new IllegalArgumentException("Got " + value + ", expected " + memBudgetInFrames);
            }
        }
    }

    public static LocalMemoryRequirements variableMemoryBudget(int minMemBudgetInFrames) {
        return new VariableMemoryBudget(minMemBudgetInFrames);
    }

    private static final class VariableMemoryBudget extends LocalMemoryRequirements {

        private final int minMemBudgetInFrames;

        private int memBudgetInFrames;

        private VariableMemoryBudget(int minMemBudgetInFrames) {
            this.memBudgetInFrames = this.minMemBudgetInFrames = minMemBudgetInFrames;
        }

        @Override
        public int getMinMemoryBudgetInFrames() {
            return minMemBudgetInFrames;
        }

        @Override
        public int getMemoryBudgetInFrames() {
            return memBudgetInFrames;
        }

        @Override
        public void setMemoryBudgetInFrames(int value) {
            if (value < minMemBudgetInFrames) {
                throw new IllegalArgumentException("Got " + value + ", expected " + minMemBudgetInFrames + " or more");
            }
            memBudgetInFrames = value;
        }
    }
}