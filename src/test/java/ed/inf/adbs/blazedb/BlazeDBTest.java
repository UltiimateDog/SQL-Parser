package ed.inf.adbs.blazedb;

import static ed.inf.adbs.blazedb.Helper.CSV_Equals;
import static org.junit.Assert.assertTrue;

import ed.inf.adbs.blazedb.operator.*;
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
			Operator selectOperator = new SelectOperator(scanOperator, parser.getWhereClause(), parser.getTableNames());
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
			Operator selectOperator = new SelectOperator(scanOperator, parser.getWhereClause(), parser.getTableNames());
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

	@Test
	public void Project_test2() throws IOException {
		for (int i = 0; i < 3; i++) {
			String name = "project" + (i+1);
			String nameO = "project" + (i+4);
			String name2 = "select" + (i+1);
			String outputFile = OUTPUT_DIR + File.separator + nameO + ".csv";
			String expFile = EXP_DIR + File.separator + nameO + ".csv";
			String inputFile = INPUT_DIR + File.separator + name + ".sql";
			String inputFile2 = INPUT_DIR + File.separator + name2 + ".sql";

			Parser parser = new Parser(inputFile);
			Parser parser2 = new Parser(inputFile2);
			Operator scanOperator = new ScanOperator(parser.getFromTable().toString());
			Operator selectOperator = new SelectOperator(scanOperator, parser2.getWhereClause(), parser2.getTableNames());
			Operator projectOperator = new ProjectOperator(selectOperator, parser.getSelectItems());
			BlazeDB.execute(projectOperator, outputFile);

			assertTrue(CSV_Equals(outputFile, expFile));
		}
	}

	@Test
	public void Join_test1() throws IOException {
			String name = "join1";
			String outputFile = OUTPUT_DIR + File.separator + name + ".csv";
			String expFile = EXP_DIR + File.separator + name + ".csv";
			String inputFile = INPUT_DIR + File.separator + name + ".sql";

			Parser parser = new Parser(inputFile);
			Operator scanOperator = new ScanOperator(parser.getFromTable().toString());
			Operator scanOperator2 = new ScanOperator(parser.getTableNames().get(1));
			Operator joinOperator = new JoinOperator(scanOperator, scanOperator2, parser.getWhereClause(), parser.getTableNames());
			BlazeDB.execute(joinOperator, outputFile);

			assertTrue(CSV_Equals(outputFile, expFile));
	}

	@Test
	public void Join_test2() throws IOException {
		String name = "join2";
		String outputFile = OUTPUT_DIR + File.separator + name + ".csv";
		String expFile = EXP_DIR + File.separator + name + ".csv";
		String inputFile = INPUT_DIR + File.separator + name + ".sql";

		Parser parser = new Parser(inputFile);
		Operator scanOperator = new ScanOperator(parser.getFromTable().toString());
		Operator scanOperator2 = new ScanOperator(parser.getTableNames().get(1));
		Operator joinOperator = new JoinOperator(scanOperator, scanOperator2, parser.getWhereClause(), parser.getTableNames());
		BlazeDB.execute(joinOperator, outputFile);

		assertTrue(CSV_Equals(outputFile, expFile));
	}



}
