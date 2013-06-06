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
package edu.uci.ics.asterix.api.common;

import java.util.ArrayList;
import java.util.List;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

public class AsterixClientConfig {
    @Option(name = "-optimize", usage = "Turns compiler optimizations on (if set to true) or off (if set to false). It is true by default.")
    public String optimize = "true";

    @Option(name = "-only-physical", usage = "Prints only the physical annotations, not the entire operators. It is false by default.")
    public String onlyPhysical = "false";

    @Option(name = "-execute", usage = "Executes the job produced by the compiler. It is false by default.")
    public String execute = "false";

    @Option(name = "-hyracks-job", usage = "Generates and prints the Hyracks job. It is false by default.")
    public String hyracksJob = "false";

    @Option(name = "-hyracks-port", usage = "The port used to connect to the Hyracks server.")
    public int hyracksPort = AsterixHyracksIntegrationUtil.DEFAULT_HYRACKS_CC_CLIENT_PORT;

    @Argument
    private List<String> arguments = new ArrayList<String>();

    public List<String> getArguments() {
        return arguments;
    }
}
