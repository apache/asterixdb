package edu.uci.ics.hivesterix.test.base;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.StringWriter;

import junit.framework.TestCase;

public class AbstractHivesterixTestCase extends TestCase {
	protected File queryFile;

	public AbstractHivesterixTestCase(String testName, File queryFile) {
		super(testName);
	}

	protected static void readFileToString(File file, StringBuilder buf)
			throws Exception {
		BufferedReader result = new BufferedReader(new FileReader(file));
		while (true) {
			String s = result.readLine();
			if (s == null) {
				break;
			} else {
				buf.append(s);
				buf.append('\n');
			}
		}
		result.close();
	}

	protected static void writeStringToFile(File file, StringWriter buf)
			throws Exception {
		PrintWriter result = new PrintWriter(new FileWriter(file));
		result.print(buf);
		result.close();
	}

	protected static void writeStringToFile(File file, StringBuilder buf)
			throws Exception {
		PrintWriter result = new PrintWriter(new FileWriter(file));
		result.print(buf);
		result.close();
	}

	protected static String removeExt(String fname) {
		int dot = fname.lastIndexOf('.');
		return fname.substring(0, dot);
	}
}
