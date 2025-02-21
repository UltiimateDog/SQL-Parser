package ed.inf.adbs.blazedb;

import static ed.inf.adbs.blazedb.Helper.CSV_Equals;
import static org.junit.Assert.assertTrue;

import ed.inf.adbs.blazedb.operator.Operator;
import ed.inf.adbs.blazedb.operator.ScanOperator;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Unit tests for BlazeDB.
 */
public class BlazeDBTest {
	String DB_DIR = "samples/db";
	String INPUT_DIR = "samples/testing/input";
	String OUTPUT_DIR = "samples/testing/output";
	String EXP_DIR = "samples/testing/expected";

	DatabaseCatalog catalog = DatabaseCatalog.getInstance(DB_DIR);
	
	/**z
	 * Rigorous Test :-)
	 */
	@Test
	public void Scan_Tests() {
		try {
			String[] tables = new String[]{"Course", "Enrolled", "Student"};
			for (int i = 0; i < tables.length; i++) {
				String outputFile = OUTPUT_DIR + File.separator + "scan" + (i + 1) + ".csv";
				String expFile = EXP_DIR + File.separator + "scan" + (i + 1) + ".csv";
				Operator scanOperator = new ScanOperator(tables[i]);

				BlazeDB.execute(scanOperator, outputFile);
				assertTrue(CSV_Equals(outputFile, expFile));
			}
		} catch (IOException e) {
			System.err.println("Table file not found.");
		}
	}
}
