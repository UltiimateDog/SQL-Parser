package ed.inf.adbs.blazedb;

import lombok.Getter;
import java.io.*;
import java.util.*;

/**
 * DatabaseCatalog is a singleton class that manages metadata about database tables.
 * It keeps track of table schemas and file locations by loading data from `schema.txt`.
 */
@Getter
public class DatabaseCatalog {
    @Getter
    private static DatabaseCatalog instance = null;
    private final String databaseDir;
    private final Map<String, List<String>> tableSchemas; // TableName -> Column Names
    private final Map<String, File> tableFiles; // TableName -> File object

    /**
     * Private constructor for Singleton pattern. Initializes the database directory and loads table metadata.
     *
     * @param databaseDir The directory where the database files are stored.
     */
    private DatabaseCatalog(String databaseDir) {
        this.databaseDir = databaseDir;
        this.tableSchemas = new HashMap<>();
        this.tableFiles = new HashMap<>();
        loadSchema();
    }

    /**
     * Returns the singleton instance of DatabaseCatalog, creating it if necessary.
     *
     * @param databaseDir The directory where the database files are stored.
     * @return The singleton instance of DatabaseCatalog.
     */
    public static DatabaseCatalog getInstance(String databaseDir) {
        if (instance == null) {
            instance = new DatabaseCatalog(databaseDir);
        } else if (!instance.databaseDir.equals(databaseDir)) {
            throw new IllegalStateException("DatabaseCatalog is already initialized with a different directory: " + instance.databaseDir);
        }
        return instance;
    }

    /**
     * Loads the database schema from the `schema.txt` file.
     * This method populates the `tableSchemas` and `tableFiles` maps.
     *
     * @throws RuntimeException if `schema.txt` cannot be read.
     */
    private void loadSchema() {
        File schemaFile = new File(databaseDir + File.separator + "schema.txt");

        try (BufferedReader reader = new BufferedReader(new FileReader(schemaFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\s+"); // Split by spaces
                if (parts.length < 2) {
                    throw new IllegalArgumentException("Invalid schema format in schema.txt: " + line);
                }
                String tableName = parts[0];
                List<String> columns = Arrays.asList(Arrays.copyOfRange(parts, 1, parts.length));

                tableSchemas.put(tableName, columns);
                tableFiles.put(tableName, new File(databaseDir + File.separator + "data" + File.separator + tableName + ".csv"));
            }
        } catch (IOException e) {
            throw new RuntimeException("Error loading schema.txt", e);
        }
    }

    /**
     * Retrieves the file associated with a given table.
     *
     * @param tableName The name of the table.
     * @return The File object representing the table's CSV file, or null if the table does not exist.
     */
    public File getTableFile(String tableName) {
        return tableFiles.get(tableName);
    }

    /**
     * Retrieves the schema (list of column names) for a given table.
     *
     * @param tableName The name of the table.
     * @return A list of column names, or null if the table does not exist.
     */
    public List<String> getTableSchema(String tableName) {
        return tableSchemas.get(tableName);
    }

    /**
     * Retrieves schemas for multiple tables.
     *
     * @param tableNames A list of table names.
     * @return A map where each table name is mapped to its corresponding list of column names.
     *         If a table is not found, it is mapped to an empty list.
     */
    public Map<String, List<String>> getTableSchemas(List<String> tableNames) {
        Map<String, List<String>> result = new HashMap<>();
        for (String tableName : tableNames) {
            result.put(tableName, tableSchemas.getOrDefault(tableName, Collections.emptyList()));
        }
        return result;
    }

    /**
     * Prints the database catalog metadata.
     *
     * @param showFilePath If true, also prints the file paths of the tables.
     */
    public void printCatalog(boolean showFilePath) {
        System.out.println("Database Directory: " + databaseDir);
        System.out.println("Tables:");
        for (String tableName : tableSchemas.keySet()) {
            System.out.println("- Table Name: " + tableName);
            System.out.println("  Columns: " + String.join(", ", tableSchemas.get(tableName)));
            if (showFilePath) {
                System.out.println("  File Path: " + tableFiles.get(tableName).getAbsolutePath());
            }
        }
    }
}
