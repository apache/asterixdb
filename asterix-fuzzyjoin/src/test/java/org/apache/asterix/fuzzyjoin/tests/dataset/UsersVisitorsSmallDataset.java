/**
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

package org.apache.asterix.fuzzyjoin.tests.dataset;

import java.util.NoSuchElementException;

public class UsersVisitorsSmallDataset extends AbstractDataset {
    private final int NO_RECORDS = 4;
    private final String NAME = "users-visitors-small";
    private static final String USERS_SUFFIX = "users";
    private static final String VISITORS_SUFFIX = "visitors";
    private final String PATH = NAME;
    private final float THRESHOLD = .5f;

    public UsersVisitorsSmallDataset() {
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int getNoRecords() {
        return NO_RECORDS;
    }

    @Override
    public String getPath() {
        return PATH;
    }

    @Override
    public String getSuffix(Relation relation) {
        switch (relation) {
            case R:
                return USERS_SUFFIX;
            case S:
                return VISITORS_SUFFIX;
            default:
                throw new NoSuchElementException();
        }
    }

    @Override
    public float getThreshold() {
        return THRESHOLD;
    }

}
