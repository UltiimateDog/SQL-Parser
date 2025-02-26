package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DatabaseCatalog;
import ed.inf.adbs.blazedb.Tuple;
import java.io.*;

/**
 * ScanOperator reads tuples from a table file and iterates over them.
 */
public class ScanOperator extends Operator {
    private BufferedReader reader;
    private final File tableFile;

    /**
     * Initializes the ScanOperator for a given table.
     * @param tableName The name of the table to scan.
     * @throws FileNotFoundException if the table file cannot be found.
     */
    public ScanOperator(String tableName) throws FileNotFoundException {
        DatabaseCatalog catalog = DatabaseCatalog.getInstance();
        this.tableFile = catalog.getTableFile(tableName);
        openFile();
    }

    /**
     * Opens the table file for reading.
     * @throws FileNotFoundException if the table file cannot be found.
     */
    private void openFile() throws FileNotFoundException {
        reader = new BufferedReader(new FileReader(tableFile));
    }

    /**
     * Retrieves the next tuple from the table file.
     * @return The next tuple or null if there are no more tuples.
     */
    @Override
    public Tuple getNextTuple() {
        try {
            String line = reader.readLine();
            if (line == null) {
                return null;
            }
            return new Tuple(line.split(","));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Resets the file reader to the beginning of the table file.
     */
    @Override
    public void reset() {
        try {
            reader.close();
            openFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
