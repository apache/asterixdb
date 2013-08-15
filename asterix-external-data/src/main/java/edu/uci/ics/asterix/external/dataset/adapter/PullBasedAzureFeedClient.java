package edu.uci.ics.asterix.external.dataset.adapter;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Map;

import org.json.JSONObject;

import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.table.client.CloudTableClient;
import com.microsoft.windowsazure.services.table.client.TableQuery;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.library.java.JObjects.ByteArrayAccessibleInputStream;
import edu.uci.ics.asterix.om.base.AMutableDateTime;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.AMutableInt64;
import edu.uci.ics.asterix.om.base.AMutableRecord;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.base.temporal.ADateTimeParserFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.util.ResettableByteArrayOutputStream;
import edu.uci.ics.asterix.runtime.operators.file.ADMDataParser;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParser;

public class PullBasedAzureFeedClient implements IPullBasedFeedClient {
    private final ARecordType outputType;
    private final CloudStorageAccount csa;
    private final CloudTableClient ctc;
    private Iterator<AzureTweetEntity> tweets;

    private final IAObject[] mutableFields;
    private final AMutableRecord coordRec;
    private final ARecordType coordType;
    private final AMutableRecord userRec;
    private final ARecordType userType;

    private final IValueParser dtParser;
    private final String[] dateFormats = { "EEE MMM dd kk:mm:ss XXX yyyy", "EEE MMM dd kk:mm:ss ZZZZZ yyyy" };
    private final DateFormat admDateFormat;
    private final ResettableByteArrayOutputStream rbaos;
    private final DataOutputStream dos;
    private final ADMDataParser adp;
    private final ByteArrayAccessibleInputStream baais;

    public PullBasedAzureFeedClient(CloudStorageAccount csa, ARecordType outputType) throws AsterixException {
        this.outputType = outputType;
        this.csa = csa;
        ctc = csa.createCloudTableClient();
        try {
            coordType = (ARecordType) outputType.getFieldType("coordinates");
            coordRec = new AMutableRecord(coordType, null);
            userType = (ARecordType) outputType.getFieldType("user");
            userRec = new AMutableRecord(userType, null);
            mutableFields = new IAObject[] { new AMutableInt64(-1), new AMutableInt64(-1), /*coordRec,*/
            new AMutableDateTime(-1), new AMutableInt64(-1), new AMutableString(null),
            /*new AMutableString(null),*/new AMutableInt32(-1), new AMutableString(null) /*, userRec */};
        } catch (IOException e) {
            throw new AsterixException(e);
        }
        admDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
        dtParser = ADateTimeParserFactory.INSTANCE.createValueParser();
        rbaos = new ResettableByteArrayOutputStream();
        dos = new DataOutputStream(rbaos);
        baais = new ByteArrayAccessibleInputStream(rbaos.getByteArray(), 0, 0);
        adp = new ADMDataParser();
        adp.initialize(baais, outputType, false);
    }

    @Override
    public void resetOnFailure(Exception e) throws AsterixException {
        e.printStackTrace();
    }

    @Override
    public boolean alter(Map<String, String> configuration) {
        return false;
    }

    @Override
    public void stop() {
    }

    @Override
    public InflowState nextTuple(DataOutput dataOutput) throws AsterixException {
        if (tweets == null) {
            TableQuery<AzureTweetEntity> tweetQuery = TableQuery.from("Postings", AzureTweetEntity.class);
            tweets = ctc.execute(tweetQuery).iterator();
        }

        boolean moreTweets = tweets.hasNext();
        if (moreTweets) {
            AzureTweetEntity tweet = tweets.next();
            try {
                JSONObject tjo = new JSONObject(tweet.getJSON().toString());
                System.out.println(tjo);
                tjo.remove("id");
                JSONObject utjo = tjo.getJSONObject("user");
                utjo.remove("id");
                tjo.put("user", utjo);
                String tjs = tjo.toString().replaceAll("}}", "} }");
                System.out.println(tjo.getString("id_str") + " " + utjo.getString("id_str"));
                byte[] tjb = tjs.getBytes(StandardCharsets.UTF_8);
                rbaos.reset();
                dos.write(tjb, 0, tjb.length);
                dos.flush();
                baais.setContent(rbaos.getByteArray(), 0, rbaos.getByteArray().length);
                adp.initialize(baais, outputType, false);
                //                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                //                DataOutputStream dos = new DataOutputStream(baos);
                adp.parse(dataOutput);
//                dataOutput.write(baos.toByteArray());
//
//                //                adp.parse(dataOutput);
//                ISerializerDeserializer serde = AqlSerializerDeserializerProvider.INSTANCE
//                        .getSerializerDeserializer(outputType);
//                ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
//                DataInputStream dis = new DataInputStream(bais);
//                IAObject o = (IAObject) serde.deserialize(dis);
//                System.out.println(o);
            } catch (Exception e) {
                e.printStackTrace();
                throw new AsterixException(e);
            }
        }
        return moreTweets ? InflowState.DATA_AVAILABLE : InflowState.NO_MORE_DATA;
    }
}
