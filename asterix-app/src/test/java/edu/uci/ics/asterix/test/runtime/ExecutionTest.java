package edu.uci.ics.asterix.test.runtime;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import edu.uci.ics.asterix.api.common.AsterixHyracksIntegrationUtil;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.external.dataset.adapter.FileSystemBasedAdapter;
import edu.uci.ics.asterix.external.util.IdentitiyResolverFactory;
import edu.uci.ics.asterix.test.aql.TestsUtils;
import edu.uci.ics.asterix.testframework.context.TestCaseContext;

/**
 * Runs the runtime test cases under 'asterix-app/src/test/resources/runtimets'.
 */
//@RunWith(Parameterized.class)
public class ExecutionTest {
	private static final String PATH_ACTUAL = "rttest/";
	private static final String PATH_BASE = "src/test/resources/runtimets/";

	private static final String TEST_CONFIG_FILE_NAME = "asterix-build-configuration.xml";
	private static final String[] ASTERIX_DATA_DIRS = new String[] { "nc1data",
			"nc2data" };

	private static List<TestCaseContext> testCaseCollection;

	@BeforeClass
	public static void setUp() throws Exception {
		System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY,
				TEST_CONFIG_FILE_NAME);
		System.setProperty(GlobalConfig.WEB_SERVER_PORT_PROPERTY, "19002");
		File outdir = new File(PATH_ACTUAL);
		outdir.mkdirs();

		File log = new File("asterix_logs");
		if (log.exists()) {
			FileUtils.deleteDirectory(log);
		}

		AsterixHyracksIntegrationUtil.init();

		// TODO: Uncomment when hadoop version is upgraded and adapters are
		// ported.
		HDFSCluster.getInstance().setup();

		// Set the node resolver to be the identity resolver that expects node
		// names
		// to be node controller ids; a valid assumption in test environment.
		System.setProperty(
				FileSystemBasedAdapter.NODE_RESOLVER_FACTORY_PROPERTY,
				IdentitiyResolverFactory.class.getName());
		TestCaseContext.Builder b = new TestCaseContext.Builder();
		testCaseCollection = b.build(new File(PATH_BASE));
	}

	@AfterClass
	public static void tearDown() throws Exception {
		AsterixHyracksIntegrationUtil.deinit();
		File outdir = new File(PATH_ACTUAL);
		File[] files = outdir.listFiles();
		if (files == null || files.length == 0) {
			outdir.delete();
		}
		// clean up the files written by the ASTERIX storage manager
		for (String d : ASTERIX_DATA_DIRS) {
			TestsUtils.deleteRec(new File(d));
		}

		File log = new File("asterix_logs");
		if (log.exists()) {
			FileUtils.deleteDirectory(log);
		}
		HDFSCluster.getInstance().cleanup();
	}

	@Test
	public void test() throws Exception {
		for (TestCaseContext testCaseCtx : testCaseCollection) {
			TestsUtils.executeTest(PATH_ACTUAL, testCaseCtx);
		}

	}
}
