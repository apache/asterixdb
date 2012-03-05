/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.metadata.entities;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.asterix.metadata.bootstrap.MetadataConstants;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class AsterixBuiltinArtifactMap {

    public enum ARTIFACT_KIND {
        DATASET,
        DATAVERSE,
        FUNCTION,
        NODEGROUP
    }

    public static final String ARTIFACT_TYPE_DATASET = "DATASET";
    public static final String ARTIFACT_TYPE_DATAVERSE = "DATAVERSE";
    public static final String ARTIFACT_TYPE_FUNCTION = "FUNCTION";
    public static final String ARTIFACT_TYPE_NODEGROUP = "NODEGROUP";

    public static final String DATASET_DATASETS = "Dataset";
    public static final String DATASET_INDEX = "Index";
    public static final String DATASET_NODEGROUP = "NodeGroup";

    public static final String DATAVERSE_METADATA = "Metadata";

    public static final String NODEGROUP_DEFAULT = MetadataConstants.METADATA_DEFAULT_NODEGROUP_NAME;

    private static final Map<ARTIFACT_KIND, Set<String>> builtinArtifactMap = new HashMap<ARTIFACT_KIND, Set<String>>();

    static {
        Set<String> datasets = new HashSet<String>();
        datasets.add(DATASET_DATASETS);
        datasets.add(DATASET_INDEX);
        datasets.add(DATASET_NODEGROUP);
        builtinArtifactMap.put(ARTIFACT_KIND.DATASET, datasets);

        Set<String> dataverses = new HashSet<String>();
        dataverses.add(DATAVERSE_METADATA);
        builtinArtifactMap.put(ARTIFACT_KIND.DATAVERSE, dataverses);

        Set<String> nodeGroups = new HashSet<String>();
        nodeGroups.add(NODEGROUP_DEFAULT);
        builtinArtifactMap.put(ARTIFACT_KIND.NODEGROUP, nodeGroups);

    }

    public static boolean isSystemProtectedArtifact(ARTIFACT_KIND kind, Object artifactIdentifier) {
        switch (kind) {
            case NODEGROUP:
            case DATASET:
            case DATAVERSE:
                return builtinArtifactMap.get(kind).contains((String) artifactIdentifier);

            case FUNCTION:
                return AsterixBuiltinFunctions.isBuiltinCompilerFunction((FunctionIdentifier) artifactIdentifier);
            default:
                return false;
        }
    }
}
