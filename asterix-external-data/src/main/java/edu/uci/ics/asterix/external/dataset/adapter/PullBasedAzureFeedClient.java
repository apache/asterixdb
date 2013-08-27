package edu.uci.ics.asterix.external.dataset.adapter;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;

import org.json.JSONObject;

import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.table.client.CloudTableClient;
import com.microsoft.windowsazure.services.table.client.TableQuery;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.library.java.JObjects.ByteArrayAccessibleInputStream;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.util.ResettableByteArrayOutputStream;
import edu.uci.ics.asterix.runtime.operators.file.ADMDataParser;

public class PullBasedAzureFeedClient implements IPullBasedFeedClient {
    private final ARecordType outputType;
    private final CloudTableClient ctc;
    private Iterator<AzureTweetEntity> tweets;

    private final ResettableByteArrayOutputStream rbaos;
    private final DataOutputStream dos;
    private final ADMDataParser adp;
    private final ByteArrayAccessibleInputStream baais;

    public PullBasedAzureFeedClient(CloudStorageAccount csa, ARecordType outputType) throws AsterixException {
        this.outputType = outputType;
        ctc = csa.createCloudTableClient();
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
                tjo.remove("id");
                JSONObject utjo = tjo.getJSONObject("user");
                utjo.remove("id");
                tjo.put("user", utjo);
                String tjs = tjo.toString().replaceAll("}}", "}, \"z\":null }");
                System.out.println(tjo.getString("id_str") + " " + utjo.getString("id_str"));
                byte[] tjb = tjs.getBytes(StandardCharsets.UTF_8);
                rbaos.reset();
                dos.write(tjb, 0, tjb.length);
                dos.flush();
                baais.setContent(rbaos.getByteArray(), 0, tjb.length);
                adp.initialize(baais, outputType, false);
                adp.parse(dataOutput);
            } catch (Exception e) {
                e.printStackTrace();
                throw new AsterixException(e);
            }
        }
        return moreTweets ? InflowState.DATA_AVAILABLE : InflowState.NO_MORE_DATA;
    }
}
