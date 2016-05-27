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
package org.apache.hyracks.dataflow.std.join;

import java.io.Serializable;

public class MergeStatus implements Serializable {
    private static final long serialVersionUID = 1L;

    public boolean reloadingLeftFrame = false;
    public boolean continueRightLoad = false;

    public MergeBranchStatus[] branch = new MergeBranchStatus[2];

    public MergeStatus() {
        branch[0] = new MergeBranchStatus();
        branch[1] = new MergeBranchStatus();
    }
//
//    public enum BranchStatus {
//        UNKNOWN,
//        OPENED,
//        DATA_PROCESSING,
//        JOIN_PROCESSING,
//        CLOSED;
//
//        public boolean isEqualOrBefore(BranchStatus bs) {
//            return this.ordinal() <= bs.ordinal();
//        }
//    }
//
//    public boolean leftHasMore = true;
//    public boolean rightHasMore = true;
//
//    private BranchStatus leftStatus = BranchStatus.UNKNOWN;
//    private BranchStatus rightStatus = BranchStatus.UNKNOWN;
//
//    private boolean runFileWriting = false;
//    private boolean runFileReading = false;
//
//
//    public BranchStatus getLeftStatus() {
//        return leftStatus;
//    }
//
//    public BranchStatus getRightStatus() {
//        return rightStatus;
//    }
//
//    public void openLeft() {
//        leftStatus = BranchStatus.OPENED;
//    }
//
//    public void openRight() {
//        rightStatus = BranchStatus.OPENED;
//    }
//
//    public void dataLeft() {
//        leftStatus = BranchStatus.DATA_PROCESSING;
//    }
//
//    public void dataRight() {
//        rightStatus = BranchStatus.DATA_PROCESSING;
//    }
//
//    public void joinLeft() {
//        leftStatus = BranchStatus.JOIN_PROCESSING;
//    }
//
//    public void joinRight() {
//        rightStatus = BranchStatus.JOIN_PROCESSING;
//    }
//
//    public void closeLeft() {
//        leftStatus = BranchStatus.CLOSED;
//    }
//
//    public void closeRight() {
//        rightStatus = BranchStatus.CLOSED;
//    }
//
//    public boolean isRunFileWriting() {
//        return runFileWriting;
//    }
//
//    public void setRunFileWriting(boolean runFileWriting) {
//        this.runFileWriting = runFileWriting;
//    }
//
//    public boolean isRunFileReading() {
//        return runFileReading;
//    }
//
//    public void setRunFileReading(boolean runFileReading) {
//        this.runFileReading = runFileReading;
//    }

}
