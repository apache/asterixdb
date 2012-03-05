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

package edu.uci.ics.asterix.metadata.declared;

import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public class AqlIndex implements IDataSourceIndex<String, AqlSourceId> {

    private final AqlCompiledIndexDecl acid;
    private final AqlCompiledMetadataDeclarations acmd;
    private final String datasetName;

    // Every transactions needs to work with its own instance of an
    // AqlMetadataProvider.
    public AqlIndex(AqlCompiledIndexDecl acid, AqlCompiledMetadataDeclarations acmd, String datasetName) {
        this.acid = acid;
        this.acmd = acmd;
        this.datasetName = datasetName;
    }

    @Override
    public IDataSource<AqlSourceId> getDataSource() {
        try {
            AqlSourceId asid = new AqlSourceId(acmd.getDataverseName(), datasetName);
            return AqlMetadataProvider.lookupSourceInMetadata(acmd, asid);
        } catch (AlgebricksException e) {
            return null;
        }
    }

    @Override
    public String getId() {
        return acid.getIndexName();
    }

}
