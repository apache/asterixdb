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
package edu.uci.ics.hyracks.dataflow.hadoop.mapreduce;

public class HadoopTools {
    public static Object newInstance(String className) throws ClassNotFoundException, InstantiationException,
            IllegalAccessException {
        ClassLoader ctxCL = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(HadoopTools.class.getClassLoader());
            Class<?> clazz = Class.forName(className, true, HadoopTools.class.getClassLoader());
            return newInstance(clazz);
        } finally {
            Thread.currentThread().setContextClassLoader(ctxCL);
        }
    }

    public static Object newInstance(Class<?> clazz) throws InstantiationException, IllegalAccessException {
        return clazz.newInstance();
    }
}