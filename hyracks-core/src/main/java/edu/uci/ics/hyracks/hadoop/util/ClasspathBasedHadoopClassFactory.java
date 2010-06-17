/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.hadoop.util;

import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;

public class ClasspathBasedHadoopClassFactory implements IHadoopClassFactory {

	@Override
	public Mapper createMapper(String mapClassName) throws Exception {
		Class clazz = loadClass(mapClassName);
		return (Mapper)clazz.newInstance();
	}

	@Override
	public Reducer createReducer(String reduceClassName) throws Exception {
		Class clazz = loadClass(reduceClassName);
		return (Reducer)clazz.newInstance();
	}

	@Override
	public Class loadClass(String className) throws Exception {
		Class clazz = Class.forName(className);
		return clazz;
	}

	
}
