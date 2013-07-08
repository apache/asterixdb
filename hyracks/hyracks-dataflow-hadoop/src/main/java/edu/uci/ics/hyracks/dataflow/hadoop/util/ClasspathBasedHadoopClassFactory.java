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
package edu.uci.ics.hyracks.dataflow.hadoop.util;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

public class ClasspathBasedHadoopClassFactory implements IHadoopClassFactory {

    @Override
    public Object createMapper(String mapClassName, JobConf conf) throws Exception {
        Class clazz = loadClass(mapClassName);
        return ReflectionUtils.newInstance(clazz, conf);
    }

    @Override
    public Object createReducer(String reduceClassName, JobConf conf) throws Exception {
        Class clazz = loadClass(reduceClassName);
        return ReflectionUtils.newInstance(clazz, conf);
    }

    @Override
    public Class loadClass(String className) throws Exception {
        Class clazz = Class.forName(className);
        return clazz;
    }

}
