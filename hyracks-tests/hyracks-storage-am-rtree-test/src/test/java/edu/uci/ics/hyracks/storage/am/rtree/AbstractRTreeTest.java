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

package edu.uci.ics.hyracks.storage.am.rtree;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.AfterClass;

public abstract class AbstractRTreeTest {

	protected final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
			"ddMMyy-hhmmssSS");
	protected final static String tmpDir = System.getProperty("java.io.tmpdir");
	protected final static String sep = System.getProperty("file.separator");
	protected final static String fileName = tmpDir + sep
			+ simpleDateFormat.format(new Date());

	protected void print(String str) {
		System.err.print(str);
	}

	@AfterClass
	public static void cleanup() throws Exception {
		File f = new File(fileName);
		f.deleteOnExit();
	}
}
