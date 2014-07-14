package edu.uci.ics.asterix.metadata.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Dataverse;

/**
 * This is a singelton class used to maintain the version of each external dataset with indexes
 * It should be consolidated once a better global dataset lock management is introduced.
 * 
 * @author alamouda
 */
public class ExternalDatasetsRegistry {
    public static ExternalDatasetsRegistry INSTANCE = new ExternalDatasetsRegistry();
    private HashMap<String, HashMap<String, ExternalDatasetAccessManager>> globalRegister;

    private ExternalDatasetsRegistry() {
        globalRegister = new HashMap<String, HashMap<String, ExternalDatasetAccessManager>>();
    }

    /**
     * Get the current version of the dataset
     * 
     * @param dataset
     * @return
     */
    public int getDatasetVersion(Dataset dataset) {
        HashMap<String, ExternalDatasetAccessManager> dataverseRegister;
        ExternalDatasetAccessManager datasetAccessMgr;
        synchronized (this) {
            dataverseRegister = globalRegister.get(dataset.getDataverseName());
            if (dataverseRegister == null) {
                // Create a register for the dataverse, and put the dataset their with the initial value of 0
                dataverseRegister = new HashMap<String, ExternalDatasetAccessManager>();
                datasetAccessMgr = new ExternalDatasetAccessManager();
                dataverseRegister.put(dataset.getDatasetName(), datasetAccessMgr);
                globalRegister.put(dataset.getDataverseName(), dataverseRegister);
            } else {
                datasetAccessMgr = dataverseRegister.get(dataset.getDatasetName());
                if (datasetAccessMgr == null) {
                    datasetAccessMgr = new ExternalDatasetAccessManager();
                    dataverseRegister.put(dataset.getDatasetName(), datasetAccessMgr);
                }
            }
        }
        return datasetAccessMgr.getVersion();
    }

    public int getAndLockDatasetVersion(Dataset dataset, AqlMetadataProvider metadataProvider) {

        Map<String, Integer> locks = null;
        String lockKey = dataset.getDataverseName() + "." + dataset.getDatasetName();
        // check first if the lock was aquired already
        locks = metadataProvider.getLocks();
        if (locks == null) {
            locks = new HashMap<String, Integer>();
            metadataProvider.setLocks(locks);
        } else {
            // if dataset was accessed already by this job, return the registered version
            Integer version = locks.get(lockKey);
            if (version != null) {
                return version;
            }
        }

        HashMap<String, ExternalDatasetAccessManager> dataverseRegister;
        ExternalDatasetAccessManager datasetAccessMgr;
        dataverseRegister = globalRegister.get(dataset.getDataverseName());
        if (dataverseRegister == null) {
            synchronized (this) {
                // A second time synchronized just to make sure
                dataverseRegister = globalRegister.get(dataset.getDataverseName());
                if (dataverseRegister == null) {
                    // Create a register for the dataverse, and put the dataset their with the initial value of 0
                    dataverseRegister = new HashMap<String, ExternalDatasetAccessManager>();
                    globalRegister.put(dataset.getDataverseName(), dataverseRegister);
                }
            }
        }

        datasetAccessMgr = dataverseRegister.get(dataset.getDatasetName());
        if (datasetAccessMgr == null) {
            synchronized (this) {
                // a second time synchronized just to make sure
                datasetAccessMgr = dataverseRegister.get(dataset.getDatasetName());
                if (datasetAccessMgr == null) {
                    datasetAccessMgr = new ExternalDatasetAccessManager();
                    dataverseRegister.put(dataset.getDatasetName(), datasetAccessMgr);
                }
            }
        }

        // aquire the correct lock
        int version = datasetAccessMgr.queryBegin();
        locks.put(lockKey, version);
        return version;
    }

    public void refreshBegin(Dataset dataset) {
        HashMap<String, ExternalDatasetAccessManager> dataverseRegister;
        ExternalDatasetAccessManager datasetAccessMgr;
        synchronized (this) {
            dataverseRegister = globalRegister.get(dataset.getDataverseName());
            if (dataverseRegister == null) {
                // Create a register for the dataverse, and put the dataset their with the initial value of 0
                dataverseRegister = new HashMap<String, ExternalDatasetAccessManager>();
                datasetAccessMgr = new ExternalDatasetAccessManager();
                dataverseRegister.put(dataset.getDatasetName(), datasetAccessMgr);
                globalRegister.put(dataset.getDataverseName(), dataverseRegister);
            } else {
                datasetAccessMgr = dataverseRegister.get(dataset.getDatasetName());
                if (datasetAccessMgr == null) {
                    datasetAccessMgr = new ExternalDatasetAccessManager();
                    dataverseRegister.put(dataset.getDatasetName(), datasetAccessMgr);
                }
            }
        }
        // aquire the correct lock
        datasetAccessMgr.refreshBegin();
    }

    public synchronized void removeDatasetInfo(Dataset dataset) {
        HashMap<String, ExternalDatasetAccessManager> dataverseRegister = globalRegister
                .get(dataset.getDataverseName());
        if (dataverseRegister != null) {
            dataverseRegister.remove(dataset.getDatasetName());
        }
    }

    public synchronized void removeDataverse(Dataverse dataverse) {
        globalRegister.remove(dataverse.getDataverseName());
    }

    public void refreshEnd(Dataset dataset, boolean success) {
        HashMap<String, ExternalDatasetAccessManager> dataverseRegistry = globalRegister
                .get(dataset.getDataverseName());
        ExternalDatasetAccessManager datasetLockManager = dataverseRegistry.get(dataset.getDatasetName());
        datasetLockManager.refreshEnd(success);
    }

    public void buildIndexBegin(Dataset dataset) {
        HashMap<String, ExternalDatasetAccessManager> dataverseRegister;
        ExternalDatasetAccessManager datasetAccessMgr;
        synchronized (this) {
            dataverseRegister = globalRegister.get(dataset.getDataverseName());
            if (dataverseRegister == null) {
                // Create a register for the dataverse, and put the dataset their with the initial value of 0
                dataverseRegister = new HashMap<String, ExternalDatasetAccessManager>();
                datasetAccessMgr = new ExternalDatasetAccessManager();
                dataverseRegister.put(dataset.getDatasetName(), datasetAccessMgr);
                globalRegister.put(dataset.getDataverseName(), dataverseRegister);
            } else {
                datasetAccessMgr = dataverseRegister.get(dataset.getDatasetName());
                if (datasetAccessMgr == null) {
                    datasetAccessMgr = new ExternalDatasetAccessManager();
                    dataverseRegister.put(dataset.getDatasetName(), datasetAccessMgr);
                }
            }
        }
        datasetAccessMgr.buildIndexBegin();
    }

    public void buildIndexEnd(Dataset dataset) {
        HashMap<String, ExternalDatasetAccessManager> dataverseRegistry = globalRegister
                .get(dataset.getDataverseName());
        ExternalDatasetAccessManager datasetLockManager = dataverseRegistry.get(dataset.getDatasetName());
        datasetLockManager.buildIndexEnd();
    }

    public void releaseAcquiredLocks(AqlMetadataProvider metadataProvider) {
        Map<String, Integer> locks = metadataProvider.getLocks();
        if (locks == null) {
            return;
        } else {
            // if dataset was accessed already by this job, return the registered version
            Set<Entry<String, Integer>> aquiredLocks = locks.entrySet();
            for (Entry<String, Integer> entry : aquiredLocks) {
                //Get dataverse name
                String dvName = entry.getKey().substring(0, entry.getKey().indexOf("."));
                String dsName = entry.getKey().substring(entry.getKey().indexOf(".") + 1);
                HashMap<String, ExternalDatasetAccessManager> dataverseRegistry = globalRegister.get(dvName);
                ExternalDatasetAccessManager datasetLockManager = dataverseRegistry.get(dsName);
                datasetLockManager.queryEnd(entry.getValue());
            }
            locks.clear();
        }
    }
}
