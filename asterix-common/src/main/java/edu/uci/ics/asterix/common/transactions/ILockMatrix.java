/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.common.transactions;

/**
 * @author pouria
 *         Shows: - The conflict matrix for the locking protocol (whether two
 *         lock modes conflict with each other or not on a single resource) -
 *         Whether request to convert a lock mode to a new one is a conversion
 *         (i.e. the new lock mode is stringer than the current one) or not
 *         Each lock mode is shown/interpreted as an integer
 */

public interface ILockMatrix {

    /**
     * @param mask
     *            (current/expected) lock mask on the resource
     * @param reqLockMode
     *            index of the requested lockMode
     * @return true if the lock request conflicts with the mask
     */
    public boolean conflicts(int mask, int reqLockMode);

    /**
     * @param currentLockMode
     * @param reqLockMode
     * @return true if the request is a conversion
     */
    public boolean isConversion(int currentLockMode, int reqLockMode);

}
