package edu.uci.ics.hyracks.storage.am.invertedindex;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.hyracks.dataflow.common.comm.io.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearchModifier;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.FixedSizeElementInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.OccurrenceThresholdPanicException;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.SearchResultCursor;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.TOccurrenceSearcher;
import edu.uci.ics.hyracks.storage.am.invertedindex.searchmodifiers.ConjunctiveSearchModifier;
import edu.uci.ics.hyracks.storage.am.invertedindex.searchmodifiers.EditDistanceSearchModifier;
import edu.uci.ics.hyracks.storage.am.invertedindex.searchmodifiers.JaccardSearchModifier;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IToken;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.NGramUTF8StringBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.UTF8NGramTokenFactory;

public class SearchTest extends AbstractInvIndexSearchTest {

    protected List<String> dataStrings = new ArrayList<String>();
    protected List<String> firstNames = new ArrayList<String>();
    protected List<String> lastNames = new ArrayList<String>();

    @Before
    public void start() throws Exception {
        super.start();
        tokenFactory = new UTF8NGramTokenFactory();
        tokenizer = new NGramUTF8StringBinaryTokenizer(3, false, true, false, tokenFactory);
        searcher = new TOccurrenceSearcher(stageletCtx, invIndex, tokenizer);
        resultCursor = new SearchResultCursor(searcher.createResultFrameTupleAccessor(),
                searcher.createResultTupleReference());
        generateDataStrings();
        loadData();
    }

    public void generateDataStrings() {
        firstNames.add("Kathrin");
        firstNames.add("Cathrin");
        firstNames.add("Kathryn");
        firstNames.add("Cathryn");
        firstNames.add("Kathrine");
        firstNames.add("Cathrine");
        firstNames.add("Kathryne");
        firstNames.add("Cathryne");
        firstNames.add("Katherin");
        firstNames.add("Catherin");
        firstNames.add("Katheryn");
        firstNames.add("Catheryn");
        firstNames.add("Katherine");
        firstNames.add("Catherine");
        firstNames.add("Katheryne");
        firstNames.add("Catheryne");
        firstNames.add("John");
        firstNames.add("Jack");
        firstNames.add("Jonathan");
        firstNames.add("Nathan");

        lastNames.add("Miller");
        lastNames.add("Myller");
        lastNames.add("Keller");
        lastNames.add("Ketler");
        lastNames.add("Muller");
        lastNames.add("Fuller");
        lastNames.add("Smith");
        lastNames.add("Smyth");
        lastNames.add("Smithe");
        lastNames.add("Smythe");

        // Generate all 'firstName lastName' combinations as data strings
        for (String f : firstNames) {
            for (String l : lastNames) {
                dataStrings.add(f + " " + l);
            }
        }
    }

    private class TokenIdPair implements Comparable<TokenIdPair> {
        public ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
        public DataOutputStream dos = new DataOutputStream(baaos);
        public int id;

        TokenIdPair(IToken token, int id) throws IOException {
            token.serializeToken(dos);
            this.id = id;
        }

        @Override
        public int compareTo(TokenIdPair o) {
            int cmp = btreeBinCmps[0].compare(baaos.getByteArray(), 0, baaos.getByteArray().length,
                    o.baaos.getByteArray(), 0, o.baaos.getByteArray().length);
            if (cmp == 0) {
                return id - o.id;
            } else {
                return cmp;
            }
        }
    }

    public void loadData() throws IOException {
        List<TokenIdPair> pairs = new ArrayList<TokenIdPair>();
        // generate pairs for subsequent sorting and bulk-loading
        int id = 0;
        for (String s : dataStrings) {
            ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
            DataOutputStream dos = new DataOutputStream(baaos);
            UTF8StringSerializerDeserializer.INSTANCE.serialize(s, dos);
            tokenizer.reset(baaos.getByteArray(), 0, baaos.size());
            int tokenCount = 0;
            while (tokenizer.hasNext()) {
                tokenizer.next();
                IToken token = tokenizer.getToken();
                pairs.add(new TokenIdPair(token, id));
                ++tokenCount;
            }
            ++id;
        }
        Collections.sort(pairs);

        // bulk load index
        IInvertedListBuilder invListBuilder = new FixedSizeElementInvertedListBuilder(invListTypeTraits);
        InvertedIndex.BulkLoadContext ctx = invIndex.beginBulkLoad(invListBuilder, HYRACKS_FRAME_SIZE,
                BTree.DEFAULT_FILL_FACTOR);

        for (TokenIdPair t : pairs) {
            tb.reset();
            tb.addField(t.baaos.getByteArray(), 0, t.baaos.getByteArray().length);
            IntegerSerializerDeserializer.INSTANCE.serialize(t.id, dos);
            tb.addFieldEndOffset();

            appender.reset(frame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            try {
                invIndex.bulkLoadAddTuple(ctx, tuple);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        invIndex.endBulkLoad(ctx);
    }

    /**
     * Runs a specified number of randomly picked strings from dataStrings as
     * queries. We run each query, measure it's time, and print it's results.
     * 
     */
    private void runQueries(IInvertedIndexSearchModifier searchModifier, int numQueries) throws Exception {

        rnd.setSeed(50);

        for (int i = 0; i < numQueries; i++) {

            int queryIndex = Math.abs(rnd.nextInt() % dataStrings.size());
            String queryString = dataStrings.get(queryIndex);

            queryTb.reset();
            UTF8StringSerializerDeserializer.INSTANCE.serialize(queryString, queryDos);
            queryTb.addFieldEndOffset();

            queryAppender.reset(frame, true);
            queryAppender.append(queryTb.getFieldEndOffsets(), queryTb.getByteArray(), 0, queryTb.getSize());
            queryTuple.reset(queryAccessor, 0);

            int repeats = 1;
            double totalTime = 0;
            for (int j = 0; j < repeats; j++) {
                long timeStart = System.currentTimeMillis();
                try {
                    searcher.reset();
                    searcher.search(resultCursor, queryTuple, 0, searchModifier);
                } catch (OccurrenceThresholdPanicException e) {
                    // ignore panic queries
                }
                long timeEnd = System.currentTimeMillis();
                totalTime += timeEnd - timeStart;
            }
            double avgTime = totalTime / (double) repeats;
            StringBuilder strBuilder = new StringBuilder();
            strBuilder.append(i + ": " + "\"" + queryString + "\": " + avgTime + "ms" + "\n");
            strBuilder.append("CANDIDATE RESULTS:\n");
            while (resultCursor.hasNext()) {
                resultCursor.next();
                ITupleReference resultTuple = resultCursor.getTuple();
                int id = IntegerSerializerDeserializer
                        .getInt(resultTuple.getFieldData(0), resultTuple.getFieldStart(0));
                strBuilder.append(id + " " + dataStrings.get(id));
                strBuilder.append('\n');
            }
            // remove trailing newline
            strBuilder.deleteCharAt(strBuilder.length() - 1);
            LOGGER.info(strBuilder.toString());
        }
    }

    /**
     * Runs 5 random conjunctive search queries to test the
     * ConjunctiveSearchModifier.
     * 
     */
    @Test
    public void conjunctiveQueryTest() throws Exception {
        IInvertedIndexSearchModifier searchModifier = new ConjunctiveSearchModifier();
        runQueries(searchModifier, 5);
    }

    /**
     * Runs 5 random jaccard-based search queries with thresholds 0.9, 0.8, 0.7.
     * Tests the JaccardSearchModifier.
     * 
     */
    @Test
    public void jaccardQueryTest() throws Exception {
        JaccardSearchModifier searchModifier = new JaccardSearchModifier(1.0f);

        LOGGER.info("JACCARD: " + 0.9f);
        searchModifier.setJaccThresh(0.9f);
        runQueries(searchModifier, 5);

        LOGGER.info("JACCARD: " + 0.8f);
        searchModifier.setJaccThresh(0.8f);
        runQueries(searchModifier, 5);

        LOGGER.info("JACCARD: " + 0.7f);
        searchModifier.setJaccThresh(0.7f);
        runQueries(searchModifier, 5);
    }

    /**
     * Runs 5 random edit-distance based search queries with thresholds 1, 2, 3.
     * Tests the EditDistanceSearchModifier.
     * 
     */
    @Test
    public void editDistanceQueryTest() throws Exception {
        EditDistanceSearchModifier searchModifier = new EditDistanceSearchModifier(3, 0);

        LOGGER.info("EDIT DISTANCE: " + 1);
        searchModifier.setEdThresh(1);
        runQueries(searchModifier, 5);

        LOGGER.info("EDIT DISTANCE: " + 2);
        searchModifier.setEdThresh(2);
        runQueries(searchModifier, 5);

        LOGGER.info("EDIT DISTANCE: " + 3);
        searchModifier.setEdThresh(3);
        runQueries(searchModifier, 5);
    }
}
