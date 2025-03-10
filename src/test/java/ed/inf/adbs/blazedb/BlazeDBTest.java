package ed.inf.adbs.blazedb;

import static ed.inf.adbs.blazedb.Helper.csvEquals;
import static org.junit.Assert.assertTrue;

import ed.inf.adbs.blazedb.operator.*;
import ed.inf.adbs.blazedb.utility.Parser;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
			assertTrue(csvEquals(outputFile, expFile));
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
			Operator selectOperator = new SelectOperator(scanOperator, parser, parser.getTableOrder());
			BlazeDB.execute(selectOperator, outputFile);

			assertTrue(csvEquals(outputFile, expFile));
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
			Operator selectOperator = new SelectOperator(scanOperator, parser, parser.getTableOrder());
			BlazeDB.execute(selectOperator, outputFile);

			assertTrue(csvEquals(outputFile, expFile));
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
			Operator projectOperator = new ProjectOperator(scanOperator, parser);
			BlazeDB.execute(projectOperator, outputFile);

			assertTrue(csvEquals(outputFile, expFile));
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
			Operator selectOperator = new SelectOperator(scanOperator, parser2, parser2.getTableOrder());
			Operator projectOperator = new ProjectOperator(selectOperator, parser);
			BlazeDB.execute(projectOperator, outputFile);

			assertTrue(csvEquals(outputFile, expFile));
		}
	}

	@Test
	public void Join_test1() throws IOException {
		for (int i = 0; i < 3; i++) {
			String name = "join" + (i+1);
			String outputFile = OUTPUT_DIR + File.separator + name + ".csv";
			String expFile = EXP_DIR + File.separator + name + ".csv";
			String inputFile = INPUT_DIR + File.separator + name + ".sql";

			Parser parser = new Parser(inputFile);
			Operator scanOperator = new ScanOperator(parser.getFromTable().toString());
			Operator scanOperator2 = new ScanOperator(parser.getTableOrder().get(1));
			Operator joinOperator = new JoinOperator(scanOperator, scanOperator2, parser);
			BlazeDB.execute(joinOperator, outputFile);

			assertTrue(csvEquals(outputFile, expFile));
		}
	}

	@Test
	public void Join_test2() throws IOException {
		for (int i = 0; i < 2; i++) {
			String name = "join" + (i+4);
			String outputFile = OUTPUT_DIR + File.separator + name + ".csv";
			String expFile = EXP_DIR + File.separator + name + ".csv";
			String inputFile = INPUT_DIR + File.separator + name + ".sql";

			Parser parser = new Parser(inputFile);
			Operator scanOperator = new ScanOperator(parser.getFromTable().toString());
			Operator scanOperator2 = new ScanOperator(parser.getTableOrder().get(1));
			Operator joinOperator = new JoinOperator(scanOperator, scanOperator2, parser);
			BlazeDB.execute(joinOperator, outputFile);

			assertTrue(csvEquals(outputFile, expFile));
		}
	}

	@Test
	public void Join_test3() throws IOException {
		for (int i = 0; i < 2; i++) {
			String name = "join" + (i+6);
			String outputFile = OUTPUT_DIR + File.separator + name + ".csv";
			String expFile = EXP_DIR + File.separator + name + ".csv";
			String inputFile = INPUT_DIR + File.separator + name + ".sql";

			Parser parser = new Parser(inputFile);
			Operator scanOperator = new ScanOperator(parser.getFromTable().toString());
			Operator scanOperator2 = new ScanOperator(parser.getTableOrder().get(1));
			Operator joinOperator = new JoinOperator(scanOperator, scanOperator2, parser);
			Operator projectOperator = new ProjectOperator(joinOperator, parser);
			BlazeDB.execute(projectOperator, outputFile);

			assertTrue(csvEquals(outputFile, expFile));
		}
	}

	@Test
	public void Compare_test1() throws IOException {
		Tuple tup1 = new Tuple(new String[]{"1", "2", "3", "4"});
		Tuple tup2 = new Tuple(new String[]{"1", "2", "3"});
		Tuple tup3 = new Tuple(new String[]{"1", "2", "3", "4", "5", "6", "7"});
		Tuple tup4 = new Tuple(new String[]{"1", "2", "3", "4", "5", "6"});
		Tuple tup5 = new Tuple(new String[]{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"});

		List<String> tableOrder = Arrays.asList("Student", "Enrolled", "Course");
		List<String> tableOrder2 = Arrays.asList("Course", "Enrolled", "Student");

		/*
		System.out.println(getComparedTables(tup1, tableOrder));
		System.out.println(getComparedTables(tup2, tableOrder2));
		System.out.println(getComparedTables(tup3, tableOrder));
		System.out.println(getComparedTables(tup4, tableOrder2));
		System.out.println(getComparedTables(tup5, tableOrder));
		System.out.println(getComparedTables(tup5, tableOrder2));
		*/

		assertTrue(true);
	}

	@Test
	public void Join_test4() throws IOException {
		for (int i = 0; i < 3; i++) {
			String name = "join" + (i+8);
			String outputFile = OUTPUT_DIR + File.separator + name + ".csv";
			String expFile = EXP_DIR + File.separator + name + ".csv";
			String inputFile = INPUT_DIR + File.separator + name + ".sql";

			Parser parser = new Parser(inputFile);
			Operator scanOperator = new ScanOperator(parser.getTableOrder().get(0));
			Operator scanOperator2 = new ScanOperator(parser.getTableOrder().get(1));
			Operator scanOperator3 = new ScanOperator(parser.getTableOrder().get(2));
			Operator joinOperator = new JoinOperator(scanOperator, scanOperator2, parser);
			Operator joinOperator2 = new JoinOperator(joinOperator, scanOperator3, parser);
			Operator projectOperator = new ProjectOperator(joinOperator2, parser);
			BlazeDB.execute(projectOperator, outputFile);

			assertTrue(csvEquals(outputFile, expFile));
		}
	}

	@Test
	public void Sort_test1() throws IOException {
		for (int i = 0; i < 3; i++) {
			String name = "sort" + (i+1);
			String outputFile = OUTPUT_DIR + File.separator + name + ".csv";
			String expFile = EXP_DIR + File.separator + name + ".csv";
			String inputFile = INPUT_DIR + File.separator + name + ".sql";

			Parser parser = new Parser(inputFile);
			Operator scanOperator = new ScanOperator(parser.getFromTable().toString());
			Operator sortOperator = new SortOperator(scanOperator, parser);
			BlazeDB.execute(sortOperator, outputFile);

			assertTrue(csvEquals(outputFile, expFile));
		}
	}

	@Test
	public void Sort_test2() throws IOException {
		for (int i = 0; i < 1; i++) {
			String name = "sort" + (i+4);
			String outputFile = OUTPUT_DIR + File.separator + name + ".csv";
			String expFile = EXP_DIR + File.separator + name + ".csv";
			String inputFile = INPUT_DIR + File.separator + name + ".sql";

			Parser parser = new Parser(inputFile);
			Operator scanOperator = new ScanOperator(parser.getFromTable().toString());
			Operator scanOperator2 = new ScanOperator(parser.getTableOrder().get(1));
			Operator joinOperator = new JoinOperator(scanOperator, scanOperator2, parser);
			Operator sortOperator = new SortOperator(joinOperator, parser);
			Operator projectOperator = new ProjectOperator(sortOperator, parser);
			BlazeDB.execute(projectOperator, outputFile);

			assertTrue(csvEquals(outputFile, expFile));
		}
	}

	@Test
	public void Distinct_test1() throws IOException {
		for (int i = 0; i < 1; i++) {
			String name = "distinct" + (i+1);
			String outputFile = OUTPUT_DIR + File.separator + name + ".csv";
			String expFile = EXP_DIR + File.separator + name + ".csv";
			String inputFile = INPUT_DIR + File.separator + name + ".sql";

			Parser parser = new Parser(inputFile);
			Operator scanOperator = new ScanOperator(parser.getFromTable().toString());
			Operator scanOperator2 = new ScanOperator(parser.getTableOrder().get(1));
			Operator joinOperator = new JoinOperator(scanOperator, scanOperator2, parser);
			Operator sortOperator = new SortOperator(joinOperator, parser);
			Operator projectOperator = new ProjectOperator(sortOperator, parser);
			Operator distinctOperator = new DistinctOperator(projectOperator);
			BlazeDB.execute(distinctOperator, outputFile);

			assertTrue(csvEquals(outputFile, expFile));
		}
	}

	@Test
	public void Sum_test1() throws IOException {
		for (int i = 0; i < 1; i++) {
			String name = "sum" + (i+1);
			String outputFile = OUTPUT_DIR + File.separator + name + ".csv";
			String expFile = EXP_DIR + File.separator + name + ".csv";
			String inputFile = INPUT_DIR + File.separator + name + ".sql";

			Parser parser = new Parser(inputFile);
			Operator scanOperator = new ScanOperator(parser.getFromTable().toString());
			Operator sumOperator = new SumOperator(scanOperator, parser);
			BlazeDB.execute(sumOperator, outputFile);

			assertTrue(csvEquals(outputFile, expFile));
		}
	}

	@Test
	public void Sum_test2() throws IOException {
		for (int i = 0; i < 1; i++) {
			String name = "sum" + (i+2);
			String outputFile = OUTPUT_DIR + File.separator + name + ".csv";
			String expFile = EXP_DIR + File.separator + name + ".csv";
			String inputFile = INPUT_DIR + File.separator + name + ".sql";

			Parser parser = new Parser(inputFile);
			Operator scanOperator = new ScanOperator(parser.getFromTable().toString());
			Operator scanOperator2 = new ScanOperator(parser.getTableOrder().get(1));
			Operator joinOperator = new JoinOperator(scanOperator, scanOperator2, parser);
			Operator sortOperator = new SortOperator(joinOperator, parser);
			Operator sumOperator = new SumOperator(sortOperator, parser);
			BlazeDB.execute(sumOperator, outputFile);

			assertTrue(csvEquals(outputFile, expFile));
		}
	}

	@Test
	public void Sum_test3() throws IOException {
		for (int i = 0; i < 1; i++) {
			String name = "sum" + (i+3);
			String outputFile = OUTPUT_DIR + File.separator + name + ".csv";
			String expFile = EXP_DIR + File.separator + name + ".csv";
			String inputFile = INPUT_DIR + File.separator + name + ".sql";

			Parser parser = new Parser(inputFile);
			Operator scanOperator = new ScanOperator(parser.getFromTable().toString());
			Operator scanOperator2 = new ScanOperator(parser.getTableOrder().get(1));
			Operator joinOperator = new JoinOperator(scanOperator, scanOperator2, parser);
			Operator sumOperator = new SumOperator(joinOperator, parser);
			BlazeDB.execute(sumOperator, outputFile);

			assertTrue(csvEquals(outputFile, expFile));
		}
	}

}
