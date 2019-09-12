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
package org.apache.hyracks.api.job.profiling;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.api.com.job.profiling.counters.Counter;
import org.apache.hyracks.api.job.profiling.counters.ICounter;

public class OperatorStats implements IOperatorStats {
    private static final long serialVersionUID = 6401830963367567167L;

    public final String operatorName;
    public final ICounter tupleCounter;
    public final ICounter timeCounter;
    public final ICounter diskIoCounter;

    public OperatorStats(String operatorName) {
        if (operatorName == null || operatorName.isEmpty()) {
            throw new IllegalArgumentException("operatorName must not be null or empty");
        }
        this.operatorName = operatorName;
        tupleCounter = new Counter("tupleCounter");
        timeCounter = new Counter("timeCounter");
        diskIoCounter = new Counter("diskIoCounter");
    }

    public static IOperatorStats create(DataInput input) throws IOException {
        String name = input.readUTF();
        OperatorStats operatorStats = new OperatorStats(name);
        operatorStats.readFields(input);
        return operatorStats;
    }

    @Override
    public String getName() {
        return operatorName;
    }

    @Override
    public ICounter getTupleCounter() {
        return tupleCounter;
    }

    @Override
    public ICounter getTimeCounter() {
        return timeCounter;
    }

    @Override
    public ICounter getDiskIoCounter() {
        return diskIoCounter;
    }

    @Override
    public void writeFields(DataOutput output) throws IOException {
        output.writeUTF(operatorName);
        output.writeLong(tupleCounter.get());
        output.writeLong(timeCounter.get());
        output.writeLong(diskIoCounter.get());
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        tupleCounter.set(input.readLong());
        timeCounter.set(input.readLong());
        diskIoCounter.set(input.readLong());
    }

    @Override
    public String toString() {
        return "{ " + "\"operatorName\": \"" + operatorName + "\", " + "\"" + tupleCounter.getName() + "\": "
                + tupleCounter.get() + ", \"" + timeCounter.getName() + "\": " + timeCounter.get() + " }";
    }
}
