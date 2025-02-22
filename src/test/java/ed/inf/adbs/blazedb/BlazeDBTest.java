package ed.inf.adbs.blazedb;

import static ed.inf.adbs.blazedb.Helper.CSV_Equals;
import static org.junit.Assert.assertTrue;

import ed.inf.adbs.blazedb.operator.Operator;
import ed.inf.adbs.blazedb.operator.ProjectOperator;
import ed.inf.adbs.blazedb.operator.ScanOperator;
import ed.inf.adbs.blazedb.operator.SelectOperator;
import ed.inf.adbs.blazedb.parsers.Parser;
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
	public void Scan_tests() throws IOException {
		String[] tables = new String[]{"Course", "Enrolled", "Student"};
		for (int i = 0; i < tables.length; i++) {
			String outputFile = OUTPUT_DIR + File.separator + "scan" + (i + 1) + ".csv";
			String expFile = EXP_DIR + File.separator + "scan" + (i + 1) + ".csv";
			Operator scanOperator = new ScanOperator(tables[i]);

			BlazeDB.execute(scanOperator, outputFile);
			assertTrue(CSV_Equals(outputFile, expFile));
		}
	}

	@Test
	public void Select_test1() throws IOException {
		for (int i = 0; i < 3; i++) {
			String name = "select" + (i+1);
			String outputFile = OUTPUT_DIR + File.separator + name + ".csv";
			String expFile = EXP_DIR + File.separator + name + ".csv";
			String inputFile = INPUT_DIR + File.separator + name + ".sql";

			Parser parser = new Parser(inputFile);
			Operator scanOperator = new ScanOperator(parser.getFromTable().toString());
			Operator selectOperator = new SelectOperator(scanOperator, parser.getWhereClause());
			BlazeDB.execute(selectOperator, outputFile);

			assertTrue(CSV_Equals(outputFile, expFile));
		}
	}

	@Test
	public void Select_test2() throws IOException {
		for (int i = 0; i < 2; i++) {
			String name = "select" + (i+4);
			String outputFile = OUTPUT_DIR + File.separator + name + ".csv";
			String expFile = EXP_DIR + File.separator + name + ".csv";
			String inputFile = INPUT_DIR + File.separator + name + ".sql";

			Parser parser = new Parser(inputFile);
			Operator scanOperator = new ScanOperator(parser.getFromTable().toString());
			Operator selectOperator = new SelectOperator(scanOperator, parser.getWhereClause());
			BlazeDB.execute(selectOperator, outputFile);

			assertTrue(CSV_Equals(outputFile, expFile));
		}
	}

	@Test
	public void Project_test1() throws IOException {
		for (int i = 0; i < 3; i++) {
			String name = "project" + (i+1);
			String outputFile = OUTPUT_DIR + File.separator + name + ".csv";
			String expFile = EXP_DIR + File.separator + name + ".csv";
			String inputFile = INPUT_DIR + File.separator + name + ".sql";

			Parser parser = new Parser(inputFile);
			Operator scanOperator = new ScanOperator(parser.getFromTable().toString());
			Operator projectOperator = new ProjectOperator(scanOperator, parser.getSelectItems());
			BlazeDB.execute(projectOperator, outputFile);

			assertTrue(CSV_Equals(outputFile, expFile));
		}
	}

}
