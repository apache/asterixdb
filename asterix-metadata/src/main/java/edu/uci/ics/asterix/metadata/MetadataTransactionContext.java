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

package edu.uci.ics.asterix.metadata;

import java.util.ArrayList;

import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.asterix.metadata.entities.Function;
import edu.uci.ics.asterix.metadata.entities.NodeGroup;
import edu.uci.ics.asterix.transaction.management.service.transaction.JobId;

/**
 * Used to implement serializable transactions against the MetadataCache.
 * Assumes that the MetadataNode also provides serializable transactions. If the
 * MetadataNode provides weaker guarantees than serializable, then you can
 * expect update anomalies in the MetadataManager, e.g., because the
 * MetadataCache could get polluted by a transaction reading an uncommitted
 * write of another transaction, and putting the uncommitted entity in the
 * MetadataCache. Logs the logical operations of a single transaction against
 * the metadata. Once the transaction decides to commit, its log will be forward
 * rolled against the MetadataCache. If it decides to abort, then its logs are
 * simply discarded without changing the MetadataCache. Also provides a
 * transaction-local "view" of its own uncommitted changes to the MetadataCache,
 * to allow metadata transactions to read their uncommitted writes, without
 * changing the MetadataCache. For example, think of what would happen if a
 * transactions read its own uncommitted addDataset(), causing an update to the
 * MetadataCache (the MetadataCache is immediately updated on reads, following
 * the assumption that the MetadataNode won't allow reading uncommitted values).
 * Another transaction might see this uncommitted dataset in the MetadataCache.
 * This class is not thread safe. We assume that a metadata transaction consists
 * of only one thread.
 */
public class MetadataTransactionContext extends MetadataCache {

	// Keeps track of deleted metadata entities.
	// An entity can either be in the droppedCache or in the inherited members
	// of MetadataCache (the "added" entities).
	// The APIs in this class make sure that these two caches are kept in sync.
	protected MetadataCache droppedCache = new MetadataCache();

	protected ArrayList<MetadataLogicalOperation> opLog = new ArrayList<MetadataLogicalOperation>();
	private final JobId jobId;

	public MetadataTransactionContext(JobId jobId) {
		this.jobId = jobId;
	}

	public JobId getJobId() {
		return jobId;
	}

	public void addDataverse(Dataverse dataverse) {
		droppedCache.dropDataverse(dataverse);
		logAndApply(new MetadataLogicalOperation(dataverse, true));
	}

	public void addDataset(Dataset dataset) {
		droppedCache.dropDataset(dataset);
		logAndApply(new MetadataLogicalOperation(dataset, true));
	}

	public void addDatatype(Datatype datatype) {
		droppedCache.dropDatatype(datatype);
		logAndApply(new MetadataLogicalOperation(datatype, true));
	}

	public void addNogeGroup(NodeGroup nodeGroup) {
		droppedCache.dropNodeGroup(nodeGroup);
		logAndApply(new MetadataLogicalOperation(nodeGroup, true));
	}

	public void addFunction(Function function) {
		droppedCache.dropFunction(function);
		logAndApply(new MetadataLogicalOperation(function, true));
	}

	public void dropDataverse(String dataverseName) {
		Dataverse dataverse = new Dataverse(dataverseName, null);
		droppedCache.addDataverseIfNotExists(dataverse);
		logAndApply(new MetadataLogicalOperation(dataverse, false));
	}

	public void dropDataset(String dataverseName, String datasetName) {
		Dataset dataset = new Dataset(dataverseName, datasetName, null, null,
				null, -1);
		droppedCache.addDatasetIfNotExists(dataset);
		logAndApply(new MetadataLogicalOperation(dataset, false));
	}

	public void dropDataDatatype(String dataverseName, String datatypeName) {
		Datatype datatype = new Datatype(dataverseName, datatypeName, null,
				false);
		droppedCache.addDatatypeIfNotExists(datatype);
		logAndApply(new MetadataLogicalOperation(datatype, false));
	}

	public void dropNodeGroup(String nodeGroupName) {
		NodeGroup nodeGroup = new NodeGroup(nodeGroupName, null);
		droppedCache.addNodeGroupIfNotExists(nodeGroup);
		logAndApply(new MetadataLogicalOperation(nodeGroup, false));
	}

	public void dropFunction(String dataverseName, String functionName,
			int arity) {
		Function function = new Function(dataverseName, functionName, arity,
				null, null);
		droppedCache.addFunctionIfNotExists(function);
		logAndApply(new MetadataLogicalOperation(function, false));
	}

	public void logAndApply(MetadataLogicalOperation op) {
		opLog.add(op);
		doOperation(op);
	}

	public boolean dataverseIsDropped(String dataverseName) {
		return droppedCache.getDataverse(dataverseName) != null;
	}

	public boolean datasetIsDropped(String dataverseName, String datasetName) {
		if (droppedCache.getDataverse(dataverseName) != null) {
			return true;
		}
		return droppedCache.getDataset(dataverseName, datasetName) != null;
	}

	public boolean datatypeIsDropped(String dataverseName, String datatypeName) {
		if (droppedCache.getDataverse(dataverseName) != null) {
			return true;
		}
		return droppedCache.getDatatype(dataverseName, datatypeName) != null;
	}

	public boolean nodeGroupIsDropped(String nodeGroup) {
		return droppedCache.getNodeGroup(nodeGroup) != null;
	}


	public boolean functionIsDropped(String dataverseName, String functionName,
			int arity) {
		return droppedCache.getFunction(dataverseName, functionName, arity) != null;
	}
	
	public ArrayList<MetadataLogicalOperation> getOpLog() {
		return opLog;
	}

	@Override
	public void clear() {
		super.clear();
		droppedCache.clear();
		opLog.clear();
	}
}
