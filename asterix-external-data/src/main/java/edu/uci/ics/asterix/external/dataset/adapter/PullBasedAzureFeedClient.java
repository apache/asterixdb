package edu.uci.ics.asterix.external.dataset.adapter;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONException;
import org.json.JSONObject;

import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.table.client.CloudTableClient;
import com.microsoft.windowsazure.services.table.client.TableQuery;
import com.microsoft.windowsazure.services.table.client.TableServiceEntity;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.library.java.JObjects.ByteArrayAccessibleInputStream;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.util.ResettableByteArrayOutputStream;
import edu.uci.ics.asterix.runtime.operators.file.ADMDataParser;

public class PullBasedAzureFeedClient implements IPullBasedFeedClient {
    private final String tableName;
    private final ARecordType outputType;
    private final CloudTableClient ctc;
    private final TableQuery<? extends TableServiceEntity> tableQuery;
    private Iterator<? extends TableServiceEntity> entityIt;

    private final Pattern arrayPattern = Pattern.compile("\\[(?<vals>.*)\\]");
    private final Pattern int32Pattern = Pattern.compile(":(?<int>\\d+)(,|})");
    private final Pattern doubleWithEndingZeroPattern = Pattern.compile("\\d+\\.(?<zero>0)(,|})");

    private final ResettableByteArrayOutputStream rbaos;
    private final DataOutputStream dos;
    private final ADMDataParser adp;
    private final ByteArrayAccessibleInputStream baais;

    public PullBasedAzureFeedClient(CloudStorageAccount csa, ARecordType outputType, String tableName)
            throws AsterixException {
        this.tableName = tableName;
        this.outputType = outputType;
        this.tableQuery = TableQuery.from(tableName, classFromString(tableName));
        this.ctc = csa.createCloudTableClient();
        rbaos = new ResettableByteArrayOutputStream();
        dos = new DataOutputStream(rbaos);
        baais = new ByteArrayAccessibleInputStream(rbaos.getByteArray(), 0, 0);
        adp = new ADMDataParser();
        adp.initialize(baais, outputType, false);
    }

    private Class<? extends TableServiceEntity> classFromString(String tableName) {
        return tableName.equals("Postings") ? AzureTweetEntity.class : AzureTweetMetadataEntity.class;
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
        if (entityIt == null) {
            entityIt = ctc.execute(tableQuery).iterator();
        }

        boolean moreTweets = entityIt.hasNext();
        if (moreTweets) {
            try {
                String json = getJSONString().replaceAll("}}", "}, \"z\":null }");
                System.out.println(json);
                byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);
                rbaos.reset();
                dos.write(jsonBytes, 0, jsonBytes.length);
                dos.flush();
                baais.setContent(rbaos.getByteArray(), 0, jsonBytes.length);
                adp.initialize(baais, outputType, false);
                adp.parse(dataOutput);
            } catch (Exception e) {
                e.printStackTrace();
                throw new AsterixException(e);
            }
        }
        return moreTweets ? InflowState.DATA_AVAILABLE : InflowState.NO_MORE_DATA;
    }

    private String getJSONString() throws JSONException {
        if (tableName.equals("Postings")) {
            AzureTweetEntity tweet = (AzureTweetEntity) entityIt.next();
            JSONObject tjo = new JSONObject(tweet.getJSON().toString());
            tjo.remove("id");
            JSONObject utjo = tjo.getJSONObject("user");
            utjo.remove("id");
            tjo.put("user", utjo);
            return tjo.toString();
        } else if (tableName.equals("PostingMetadata")) {
            AzureTweetMetadataEntity tweetMD = (AzureTweetMetadataEntity) entityIt.next();
            JSONObject tmdjo = new JSONObject();
            tmdjo.put("created_at", stripTillColon(tweetMD.getCreationTimestamp()).replaceAll("\"", ""));
            tmdjo.put("posting_type", stripTillColon(tweetMD.getPostingType()));
            List<String> productIdList = Arrays.asList(extractArray(tweetMD.getProductId()));
            tmdjo.put("product_id", productIdList);
            if (tweetMD.getEthnicity() != null) {
                tmdjo.put("ethnicity", new JSONObject(stripTillColon(tweetMD.getEthnicity())));
            }
            if (tweetMD.getGender() != null) {
                tmdjo.put("gender", new JSONObject(stripTillColon(tweetMD.getGender())));
            }
            if (tweetMD.getLocation() != null) {
                String locStr = stripTillColon(tweetMD.getLocation());
                Matcher m = int32Pattern.matcher(locStr);
                while (m.find()) {
                    locStr = locStr.replace(m.group("int"), m.group("int") + ".01");
                }
                m = doubleWithEndingZeroPattern.matcher(locStr);
                while (m.find()) {
                    locStr = locStr.replace(m.group("zero"), "01");
                }
                tmdjo.put("location", new JSONObject(locStr));
            }
            if (tweetMD.getSentiment() != null) {
                tmdjo.put("sentiment", stripTillColon(tweetMD.getSentiment()));
            }
            return tmdjo.toString();
        } else {
            throw new IllegalArgumentException();
        }
    }

    private String stripTillColon(String str) {
        return str.substring(str.indexOf(':') + 1);
    }

    private String[] extractArray(String str) {
        Matcher m = arrayPattern.matcher(str);
        m.find();
        return m.group("vals").replaceAll("\\s", "").split(",");
    }

    public static void main(String[] args) throws Exception {
        Pattern int32Pattern = Pattern.compile(/*"(?<int>\\d+)(?!\\.\\d*,)"*/":(?<int>\\d+)(,|})");
        String locStr = "\"location\":{\"Latitude\":52.25,\"Longitude\":21}";
        Matcher m = int32Pattern.matcher(locStr);
        while (m.find()) {
            locStr = locStr.replace(m.group("int"), m.group("int") + ".0");
        }
        System.out.println(locStr);
    }
}
