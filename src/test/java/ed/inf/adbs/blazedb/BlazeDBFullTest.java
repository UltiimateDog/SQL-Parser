package ed.inf.adbs.blazedb;

import static ed.inf.adbs.blazedb.Helper.CSV_Equals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.io.File;
import java.io.IOException;


/**
 * Unit tests for BlazeDB.
 */
public class BlazeDBFullTest {
    String DB_DIR = "samples/db";
    String INPUT_DIR = "samples/testing/input";
    String OUTPUT_DIR = "samples/testing/outputFull";
    String EXP_DIR = "samples/testing/expected";
    String INPUT_DIR2 = "samples/input";
    String OUTPUT_DIR2 = "samples/output";
    String EXP_DIR2 = "samples/expected_output";

    @Test
    public void testQueryPlan() throws IOException {
        File inputDir = new File(INPUT_DIR);
        File[] queryFiles = inputDir.listFiles((dir, name) -> name.endsWith(".sql"));

        if (queryFiles == null) {
            throw new IOException("Input directory does not exist or is empty.");
        }

        for (File queryFile : queryFiles) {
            String queryName = queryFile.getName().replace(".sql", "");
            String inputFile = queryFile.getAbsolutePath();
            String outputFile = OUTPUT_DIR + File.separator + queryName + ".csv";
            String expFile = EXP_DIR + File.separator + queryName + ".csv";

            System.out.println("Running test for: " + queryName);

            BlazeDB.main(new String[]{DB_DIR, inputFile, outputFile});

            assertTrue("Mismatch in output for: " + queryName, CSV_Equals(outputFile, expFile));
        }
    }

    @Test
    public void testQueryPlan2() throws IOException {
        File inputDir = new File(INPUT_DIR2);
        File[] queryFiles = inputDir.listFiles((dir, name) -> name.endsWith(".sql"));

        if (queryFiles == null) {
            throw new IOException("Input directory does not exist or is empty.");
        }

        for (File queryFile : queryFiles) {
            String queryName = queryFile.getName().replace(".sql", "");
            String inputFile = queryFile.getAbsolutePath();
            String outputFile = OUTPUT_DIR2 + File.separator + queryName + ".csv";
            String expFile = EXP_DIR2 + File.separator + queryName + ".csv";

            System.out.println("Running test for: " + queryName);

            BlazeDB.main(new String[]{DB_DIR, inputFile, outputFile});

            assertTrue("Mismatch in output for: " + queryName, CSV_Equals(outputFile, expFile));
        }
    }
}
