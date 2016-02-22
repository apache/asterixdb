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
package org.apache.asterix.external.classad;

public class CaseInsensitiveString implements Comparable<CaseInsensitiveString> {
    private String aString;

    public String get() {
        return aString;
    }

    @Override
    public String toString() {
        return aString;
    }

    public void set(String aString) {
        this.aString = aString;
    }

    public CaseInsensitiveString(String aString) {
        this.aString = aString;
    }

    public CaseInsensitiveString() {
        this.aString = null;
    }

    @Override
    public int compareTo(CaseInsensitiveString o) {
        return aString.compareToIgnoreCase(o.aString);
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof CaseInsensitiveString) ? aString.equalsIgnoreCase(((CaseInsensitiveString) o).aString)
                : false;
    }

    @Override
    public int hashCode() {
        return aString.toLowerCase().hashCode();
    }

}
