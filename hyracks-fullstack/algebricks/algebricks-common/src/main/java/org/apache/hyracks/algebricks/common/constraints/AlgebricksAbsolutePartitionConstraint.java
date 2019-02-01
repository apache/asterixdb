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
package org.apache.hyracks.algebricks.common.constraints;

import java.util.Arrays;
import java.util.Random;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.ErrorCode;

public class AlgebricksAbsolutePartitionConstraint extends AlgebricksPartitionConstraint {
    private final String[] locations;
    private final String[] sortedLocations;

    public AlgebricksAbsolutePartitionConstraint(String[] locations) {
        this.locations = locations;
        sortedLocations = locations.clone();
        Arrays.sort(sortedLocations);
    }

    public static AlgebricksAbsolutePartitionConstraint randomLocation(String[] locations) {
        int randomIndex = new Random().nextInt(locations.length);
        return new AlgebricksAbsolutePartitionConstraint(new String[] { locations[randomIndex] });
    }

    @Override
    public PartitionConstraintType getPartitionConstraintType() {
        return PartitionConstraintType.ABSOLUTE;
    }

    public String[] getLocations() {
        return locations;
    }

    @Override
    public String toString() {
        return getPartitionConstraintType().toString() + ':' + Arrays.toString(locations);
    }

    @Override
    public AlgebricksPartitionConstraint compose(AlgebricksPartitionConstraint that) throws AlgebricksException {
        switch (that.getPartitionConstraintType()) {
            case COUNT:
                AlgebricksCountPartitionConstraint thatCount = (AlgebricksCountPartitionConstraint) that;
                if (locations.length <= thatCount.getCount()) {
                    return this;
                }
                break;
            case ABSOLUTE:
                AlgebricksAbsolutePartitionConstraint thatAbsolute = (AlgebricksAbsolutePartitionConstraint) that;
                if (Arrays.equals(sortedLocations, thatAbsolute.sortedLocations)) {
                    return this;
                }
                break;
        }

        throw AlgebricksException.create(ErrorCode.CANNOT_COMPOSE_PART_CONSTRAINTS, toString(), that.toString());
    }
}
