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

package edu.uci.ics.hyracks.storage.am.invertedindex;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.PageAllocationException;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearchModifier;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.FixedSizeElementInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.OccurrenceThresholdPanicException;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.SearchResultCursor;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.TOccurrenceSearcher;
import edu.uci.ics.hyracks.storage.am.invertedindex.searchmodifiers.ConjunctiveSearchModifier;
import edu.uci.ics.hyracks.storage.am.invertedindex.searchmodifiers.JaccardSearchModifier;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.DelimitedUTF8StringBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.UTF8WordTokenFactory;

/**
 * The purpose of this test is to evaluate the performance of searches against
 * an inverted index. First, we generate random <token, id> pairs sorted on
 * token, which are bulk loaded into an inverted index. Next, we build random
 * queries from a list of predefined tokens in the index, and measure the
 * performance of executing them with different search modifiers. We test the
 * ConjunctiveSearchModifier and the JaccardSearchModifier.
 * 
 */
public class SearchPerfTest extends AbstractInvIndexSearchTest {

	protected List<String> tokens = new ArrayList<String>();

	@Before
	public void start() throws Exception {
		super.start();
		tokenFactory = new UTF8WordTokenFactory();
		tokenizer = new DelimitedUTF8StringBinaryTokenizer(true, false,
				tokenFactory);
		searcher = new TOccurrenceSearcher(taskCtx, invIndex, tokenizer);
		resultCursor = new SearchResultCursor(
				searcher.createResultFrameTupleAccessor(),
				searcher.createResultTupleReference());
		loadData();
	}

	public void loadData() throws HyracksDataException, TreeIndexException, PageAllocationException {
		tokens.add("compilers");
		tokens.add("computer");
		tokens.add("databases");
		tokens.add("fast");
		tokens.add("hyracks");
		tokens.add("major");
		tokens.add("science");
		tokens.add("systems");
		tokens.add("university");

		for (int i = 0; i < tokens.size(); i++) {
			checkInvLists.add(new ArrayList<Integer>());
		}

		// for generating length-skewed inverted lists
		int addProb = 0;
		int addProbStep = 10;

		IInvertedListBuilder invListBuilder = new FixedSizeElementInvertedListBuilder(
				invListTypeTraits);
		InvertedIndex.BulkLoadContext ctx = invIndex.beginBulkLoad(
				invListBuilder, HYRACKS_FRAME_SIZE, BTree.DEFAULT_FILL_FACTOR);

		int totalElements = 0;
		for (int i = 0; i < tokens.size(); i++) {

			addProb += addProbStep * (i + 1);
			for (int j = 0; j < maxId; j++) {
				if ((Math.abs(rnd.nextInt()) % addProb) == 0) {

					totalElements++;

					tb.reset();
					UTF8StringSerializerDeserializer.INSTANCE.serialize(
							tokens.get(i), dos);
					tb.addFieldEndOffset();
					IntegerSerializerDeserializer.INSTANCE.serialize(j, dos);
					tb.addFieldEndOffset();

					checkInvLists.get(i).add(j);

					appender.reset(frame, true);
					appender.append(tb.getFieldEndOffsets(), tb.getByteArray(),
							0, tb.getSize());

					tuple.reset(accessor, 0);

					try {
						invIndex.bulkLoadAddTuple(ctx, tuple);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
		invIndex.endBulkLoad(ctx);
	}

	/**
	 * Determine the expected results with the ScanCount algorithm. The
	 * ScanCount algorithm is very simple, so we can be confident the results
	 * are correct.
	 * 
	 */
	protected void fillExpectedResults(int[] queryTokenIndexes,
			int numQueryTokens, int occurrenceThreshold) {
		// reset scan count array
		for (int i = 0; i < maxId; i++) {
			scanCountArray[i] = 0;
		}

		// count occurrences
		for (int i = 0; i < numQueryTokens; i++) {
			ArrayList<Integer> list = checkInvLists.get(queryTokenIndexes[i]);
			for (int j = 0; j < list.size(); j++) {
				scanCountArray[list.get(j)]++;
			}
		}

		// check threshold
		expectedResults.clear();
		for (int i = 0; i < maxId; i++) {
			if (scanCountArray[i] >= occurrenceThreshold) {
				expectedResults.add(i);
			}
		}
	}

	/**
	 * Generates a specified number of queries. Each query consists of a set of
	 * randomly chosen tokens that are picked from the pre-defined set of
	 * tokens. We run each query, measure it's time, and verify it's results
	 * against the results produced by ScanCount, implemented in
	 * fillExpectedResults().
	 * 
	 */
	private void runQueries(IInvertedIndexSearchModifier searchModifier,
			int numQueries) throws Exception {

		rnd.setSeed(50);

		// generate random queries
		int[] queryTokenIndexes = new int[tokens.size()];
		for (int i = 0; i < numQueries; i++) {

			int numQueryTokens = Math.abs(rnd.nextInt() % tokens.size()) + 1;
			for (int j = 0; j < numQueryTokens; j++) {
				queryTokenIndexes[j] = Math.abs(rnd.nextInt() % tokens.size());
			}

			StringBuilder strBuilder = new StringBuilder();
			for (int j = 0; j < numQueryTokens; j++) {
				strBuilder.append(tokens.get(queryTokenIndexes[j]));
				if (j + 1 != numQueryTokens)
					strBuilder.append(" ");
			}

			String queryString = strBuilder.toString();

			queryTb.reset();
			UTF8StringSerializerDeserializer.INSTANCE.serialize(queryString,
					queryDos);
			queryTb.addFieldEndOffset();

			queryAppender.reset(frame, true);
			queryAppender.append(queryTb.getFieldEndOffsets(),
					queryTb.getByteArray(), 0, queryTb.getSize());
			queryTuple.reset(queryAccessor, 0);

			boolean panic = false;

			int repeats = 1;
			double totalTime = 0;
			for (int j = 0; j < repeats; j++) {
				long timeStart = System.currentTimeMillis();
				try {
					searcher.reset();
					searcher.search(resultCursor, queryTuple, 0, searchModifier);
				} catch (OccurrenceThresholdPanicException e) {
					panic = true;
				}
				long timeEnd = System.currentTimeMillis();
				totalTime += timeEnd - timeStart;
			}
			double avgTime = totalTime / (double) repeats;
			if (LOGGER.isLoggable(Level.INFO)) {
				LOGGER.info(i + ": " + "\"" + queryString + "\": " + avgTime
						+ "ms");
			}

			if (!panic) {

				fillExpectedResults(queryTokenIndexes, numQueryTokens,
						searcher.getOccurrenceThreshold());

				// verify results
				int checkIndex = 0;
				while (resultCursor.hasNext()) {
					resultCursor.next();
					ITupleReference resultTuple = resultCursor.getTuple();
					int id = IntegerSerializerDeserializer.getInt(
							resultTuple.getFieldData(0),
							resultTuple.getFieldStart(0));
					Assert.assertEquals(expectedResults.get(checkIndex)
							.intValue(), id);
					checkIndex++;
				}

				if (expectedResults.size() != checkIndex) {
					if (LOGGER.isLoggable(Level.INFO)) {
						LOGGER.info("CHECKING");
					}
					StringBuilder expectedStrBuilder = new StringBuilder();
					for (Integer x : expectedResults) {
						expectedStrBuilder.append(x + " ");
					}
					if (LOGGER.isLoggable(Level.INFO)) {
						LOGGER.info(expectedStrBuilder.toString());
					}
				}

				Assert.assertEquals(expectedResults.size(), checkIndex);
			}
		}
	}

	/**
	 * Runs 50 random conjunctive search queries to test the
	 * ConjunctiveSearchModifier.
	 * 
	 */
	@Test
	public void conjunctiveKeywordQueryTest() throws Exception {
		IInvertedIndexSearchModifier searchModifier = new ConjunctiveSearchModifier();
		runQueries(searchModifier, 50);
	}

	/**
	 * Runs 50 random jaccard-based search queries with thresholds 1.0, 0.9,
	 * 0.8, 0.7, 0.6, 0.5. Tests the JaccardSearchModifier.
	 * 
	 */
	@Test
	public void jaccardKeywordQueryTest() throws Exception {
		JaccardSearchModifier searchModifier = new JaccardSearchModifier(1.0f);

		if (LOGGER.isLoggable(Level.INFO)) {
			LOGGER.info("JACCARD: " + 1.0f);
		}
		searchModifier.setJaccThresh(1.0f);
		runQueries(searchModifier, 50);

		if (LOGGER.isLoggable(Level.INFO)) {
			LOGGER.info("JACCARD: " + 0.9f);
		}
		searchModifier.setJaccThresh(0.9f);
		runQueries(searchModifier, 50);

		if (LOGGER.isLoggable(Level.INFO)) {
			LOGGER.info("JACCARD: " + 0.8f);
		}
		searchModifier.setJaccThresh(0.8f);
		runQueries(searchModifier, 50);

		if (LOGGER.isLoggable(Level.INFO)) {
			LOGGER.info("JACCARD: " + 0.7f);
		}
		searchModifier.setJaccThresh(0.7f);
		runQueries(searchModifier, 50);

		if (LOGGER.isLoggable(Level.INFO)) {
			LOGGER.info("JACCARD: " + 0.6f);
		}
		searchModifier.setJaccThresh(0.6f);
		runQueries(searchModifier, 50);

		if (LOGGER.isLoggable(Level.INFO)) {
			LOGGER.info("JACCARD: " + 0.5f);
		}
		searchModifier.setJaccThresh(0.5f);
		runQueries(searchModifier, 50);
	}
}
