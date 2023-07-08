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
package org.apache.asterix.om.api.projection;

/**
 * Gets information about the requested columns
 */
public interface IRowProjectionInfo {
    /**
     * @param ordinal position of the requested column
     * @return column index given the ordinal number of the requested column
     */
    int getColumnIndex(int ordinal);

    /**
     * @return total number of requested columns
     */
    int getNumberOfProjectedColumns();

    /**
     * @return number of primary keys
     */
    int getNumberOfPrimaryKeys();

    /**
     * @param ordinal position of the filtered column
     * @return column index given the ordinal number of the filtered column
     */
    int getFilteredColumnIndex(int ordinal);

    /**
     * @return number of filtered columns
     */
    int getNumberOfFilteredColumns();
}
