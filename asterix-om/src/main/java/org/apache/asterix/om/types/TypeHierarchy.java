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
package org.apache.asterix.om.types;

import java.util.Hashtable;

/*
 * Author: Guangqiang Li
 * Created on Sep 24, 2009 
 */
public class TypeHierarchy {
    private static Hashtable<String, String> parentMap = new Hashtable<String, String>();
    static {
        parentMap.put("integer", "decimal");
        parentMap.put("double", "decimal");
        parentMap.put("decimal", "numeric");
    }

    public static boolean isSubType(String sub, String par) {
        String parent = parentMap.get(sub);
        if (parent != null)
            if (parent.equals(par))
                return true;
            else
                return isSubType(parent, par);
        return false;
    }
}
