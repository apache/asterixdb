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
package edu.uci.ics.asterix.om.functions;


public class AsterixFunctionIdentifier {

    private final String dataverse;
    private final AsterixFunction asterixFunction;

    public AsterixFunctionIdentifier(String dataverse, AsterixFunction asterixFunction) {
        this.dataverse = dataverse;
        this.asterixFunction = asterixFunction;
    }

    public AsterixFunctionIdentifier(String dataverse, String name, int arity) {
        this.dataverse = dataverse;
        this.asterixFunction = new AsterixFunction(name, arity);
    }

    public String toString() {
        return dataverse + ":" + asterixFunction;
    }

    public String getDataverse() {
        return dataverse;
    }

    public AsterixFunction getAsterixFunction() {
        return asterixFunction;
    }

}
