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
package edu.uci.ics.asterix.aoya;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Constants used in both Client and Application Master
 */
@InterfaceAudience.LimitedPrivate(value = { "For simplicity between Client and AM." })
@InterfaceStability.Unstable
public class AConstants {
    /**
     * Environment key name pointing to the the app master jar location
     */
    public static final String APPLICATIONMASTERJARLOCATION = "APPLICATIONMASTERJARLOCATION";

    /**
     * Environment key name denoting the file timestamp for the the app master jar.
     * Used to validate the local resource.
     */
    public static final String APPLICATIONMASTERJARTIMESTAMP = "APPLICATIONMASTERJARTIMESTAMP";

    /**
     * Environment key name denoting the file content length for the app master jar.
     * Used to validate the local resource.
     */
    public static final String APPLICATIONMASTERJARLEN = "APPLICATIONMASTERJARLEN";
    /**
     * Environment key name pointing to the Asterix distributable tar
     */
    public static final String TARLOCATION = "TARLOCATION";

    /**
     * Environment key name denoting the file timestamp for the Asterix tar.
     * Used to validate the local resource.
     */
    public static final String TARTIMESTAMP = "TARTIMESTAMP";

    /**
     * Environment key name denoting the file content length for the Asterix tar.
     * Used to validate the local resource.
     */
    public static final String TARLEN = "TARLEN";

    /**
     * Environment key name pointing to the Asterix cluster configuration file
     */
    public static final String CONFLOCATION = "CONFLOCATION";

    /**
     * Environment key name denoting the file timestamp for the Asterix config.
     * Used to validate the local resource.
     */

    public static final String CONFTIMESTAMP = "CONFTIMESTAMP";

    /**
     * Environment key name denoting the file content length for the Asterix config.
     * Used to validate the local resource.
     */

    public static final String CONFLEN = "CONFLEN";

    /**
     * Environment key name pointing to the Asterix parameters file
     */

    public static final String PARAMLOCATION = "PARAMLOCATION";

    /**
     * Environment key name denoting the file timestamp for the Asterix parameters.
     * Used to validate the local resource.
     */

    public static final String PARAMTIMESTAMP = "PARAMTIMESTAMP";

    /**
     * Environment key name denoting the file content length for the Asterix parameters.
     * Used to validate the local resource.
     */

    public static final String PARAMLEN = "PARAMLEN";

    public static final String PATHSUFFIX = "PATHSUFFIX";

    public static final String INSTANCESTORE = "INSTANCESTORE";

    public static final String RMADDRESS = "RMADDRESS";

    public static final String RMSCHEDULERADDRESS = "RMSCHEDULERADDRESS";

    public static final String DFS_BASE = "DFSBASE";

    public static final String NC_JAVA_OPTS = "NCJAVAOPTS";

    public static final String CC_JAVA_OPTS = "CCJAVAOPTS";

    public static final String NC_CONTAINER_MEM = "NC_CONTAINER_MEM";

    public static final String CC_CONTAINER_MEM = "CC_CONTAINER_MEM";

}
