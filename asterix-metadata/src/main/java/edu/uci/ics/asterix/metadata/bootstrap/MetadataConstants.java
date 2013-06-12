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

package edu.uci.ics.asterix.metadata.bootstrap;

public class MetadataConstants {

    // Name of the dataverse the metadata lives in.
    public final static String METADATA_DATAVERSE_NAME = "Metadata";
    
    // Name of the node group where metadata is stored on.
    public final static String METADATA_NODEGROUP_NAME = "MetadataGroup";
    
    // Name of the default nodegroup where internal/feed datasets will be partitioned
    // if an explicit nodegroup is not specified at the time of creation of a dataset
    public static final String METADATA_DEFAULT_NODEGROUP_NAME = "DEFAULT_NG_ALL_NODES";
}
