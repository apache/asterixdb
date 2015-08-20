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
package edu.uci.ics.hyracks.hadoop.compat.util;

import org.kohsuke.args4j.Option;

public class CompatibilityConfig {

	@Option(name = "-cluster", required = true, usage = "Defines the path to the configuration file that provides the following info: +"
			+ " (1) Address of HyracksClusterController service"
			+ " (2) Address of Hadoop namenode service")
	public String clusterConf;

	@Option(name = "-jobFiles", usage = "Comma separated list of jobFiles. "
			+ "Each job file defines the hadoop job + "
			+ "The order in the list defines the sequence in which"
			+ "the jobs are to be executed")
	public String jobFiles;

	@Option(name = "-applicationName", usage = " The application as part of which the job executes")
	public String applicationName;

	@Option(name = "-userLibs", usage = " A comma separated list of jar files that are required to be addedd to classpath when running ")
	public String userLibs;
}
