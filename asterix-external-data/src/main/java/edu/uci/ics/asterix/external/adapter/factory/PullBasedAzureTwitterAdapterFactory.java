package edu.uci.ics.asterix.external.adapter.factory;

import java.util.Map;

import edu.uci.ics.asterix.external.dataset.adapter.PullBasedAzureTwitterAdapter;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.metadata.feeds.IDatasourceAdapter;
import edu.uci.ics.asterix.metadata.feeds.ITypedAdapterFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class PullBasedAzureTwitterAdapterFactory implements ITypedAdapterFactory {

    private static final long serialVersionUID = 1L;

    private static final String OUTPUT_TYPE_KEY = "output-type";

    private ARecordType recordType;
    private Map<String, String> configuration;

    @Override
    public SupportedOperation getSupportedOperations() {
        return SupportedOperation.READ;
    }

    @Override
    public String getName() {
        return "azure_twitter";
    }

    @Override
    public AdapterType getAdapterType() {
        return AdapterType.TYPED;
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return new AlgebricksCountPartitionConstraint(1);
    }

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx) throws Exception {
        return new PullBasedAzureTwitterAdapter(configuration, ctx, recordType);
    }

    @Override
    public ARecordType getAdapterOutputType() {
        return recordType;
    }

    @Override
    public void configure(Map<String, String> configuration) throws Exception {
        this.configuration = configuration;
        String fqOutputType = configuration.get(OUTPUT_TYPE_KEY);

        if (fqOutputType == null) {
            throw new IllegalArgumentException("No output type specified");
        }
        String[] dataverseAndType = fqOutputType.split("[.]");
        String dataverse = dataverseAndType[0];
        String datatype = dataverseAndType[1];
        MetadataTransactionContext ctx = null;
        try {
            MetadataManager.INSTANCE.acquireReadLatch();
            ctx = MetadataManager.INSTANCE.beginTransaction();
            Datatype t = MetadataManager.INSTANCE.getDatatype(ctx, dataverse, datatype);
            IAType type = t.getDatatype();
            if (type.getTypeTag() != ATypeTag.RECORD) {
                throw new IllegalStateException();
            }
            recordType = (ARecordType) t.getDatatype();
            MetadataManager.INSTANCE.commitTransaction(ctx);
        } catch (Exception e) {
            if (ctx != null) {
                MetadataManager.INSTANCE.abortTransaction(ctx);
            }
            throw e;
        } finally {
            MetadataManager.INSTANCE.releaseReadLatch();
        }
        //        } else {
        //            String[] coordFieldNames = { "coordinates", "type" };
        //            IAType[] coordFieldTypes = { new AOrderedListType(BuiltinType.ADOUBLE, "coord_type"), BuiltinType.ASTRING };
        //            ARecordType coordRecType = new ARecordType("coord_rec_type", coordFieldNames, coordFieldTypes, true);
        //            AUnionType coordType = new AUnionType(Arrays.asList(new IAType[] { coordRecType, BuiltinType.ANULL }),
        //                    "coord_type");
        //
        //            AUnionType langType = new AUnionType(
        //                    Arrays.asList(new IAType[] { BuiltinType.ASTRING, BuiltinType.ANULL }), "lang_type");
        //            String[] userFieldNames = { "id", "id_str", "created_at", "followers_count", "lang", "location" };
        //            IAType[] userFieldTypes = { BuiltinType.AINT64, BuiltinType.ASTRING, BuiltinType.ASTRING,
        //                    BuiltinType.AINT32, langType, BuiltinType.ASTRING };
        //            ARecordType userRecType = new ARecordType("user_rec_type", userFieldNames, userFieldTypes, true);
        //
        //            String[] fieldNames = { "posting_id", "user_id", /*"coordinates",*/"created_at", "id", "id_str", /*"lang",*/
        //            "retweet_count", "text"/*, "user"*/};
        //            IAType[] fieldTypes = { BuiltinType.AINT64, BuiltinType.AINT32, /*coordType,*/BuiltinType.ASTRING,
        //                    BuiltinType.AINT64, BuiltinType.ASTRING, /*langType,*/BuiltinType.AINT32, BuiltinType.ASTRING /*, userRecType*/};
        //            recordType = new ARecordType("W4TwitterType", fieldNames, fieldTypes, false);
        //        }
    }
}
