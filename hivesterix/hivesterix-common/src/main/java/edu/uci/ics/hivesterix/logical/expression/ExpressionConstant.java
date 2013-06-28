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
package edu.uci.ics.hivesterix.logical.expression;

/**
 * some constants for expression
 * 
 * @author yingyib
 */
public class ExpressionConstant {

    /**
     * name space for function identifier
     */
    public static String NAMESPACE = "hive";

    /**
     * field expression: modeled as function in Algebricks
     */
    public static String FIELDACCESS = "fieldaccess";

    /**
     * null string: modeled as null in Algebricks
     */
    public static String NULL = "null";
}
