/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.om.base;

import com.esri.core.geometry.OGCStructure;
import com.esri.core.geometry.OperatorImportFromWkt;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.WktImportFlags;
import com.esri.core.geometry.ogc.OGCGeometry;

public class AMutableGeometry extends AGeometry {

    private OperatorImportFromWkt wktImporter;

    public AMutableGeometry(OGCGeometry geom) {
        super(geom);
        wktImporter = OperatorImportFromWkt.local();
    }

    public void setValue(OGCGeometry geom) {
        this.geometry = geom;
    }

    public void parseWKT(String wkt) {
        OGCStructure structure;

        structure = wktImporter.executeOGC(WktImportFlags.wktImportNonTrusted, wkt, null);
        this.geometry = OGCGeometry.createFromOGCStructure(structure, SpatialReference.create(4326));
    }
}
