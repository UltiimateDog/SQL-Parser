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

    public ScanOperator(String tableName, String databaseDir) throws FileNotFoundException {
        DatabaseCatalog catalog = DatabaseCatalog.getInstance(databaseDir);
        this.tableFile = catalog.getTableFile(tableName);
        openFile();
    }

    private void openFile() throws FileNotFoundException {
        reader = new BufferedReader(new FileReader(tableFile));
    }

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
