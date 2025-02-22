package ed.inf.adbs.blazedb;

import lombok.Getter;

import java.io.*;
import java.util.*;

/**
 * DatabaseCatalog is a singleton that stores metadata about database tables.
 * It keeps track of table schemas and file locations.
 */
@Getter
public class DatabaseCatalog {
    @Getter
    private static DatabaseCatalog instance = null;
    private final String databaseDir;
    private final Map<String, List<String>> tableSchemas; // TableName -> Column Names
    private final Map<String, File> tableFiles; // TableName -> File object

    private DatabaseCatalog(String databaseDir) {
        this.databaseDir = databaseDir;
        this.tableSchemas = new HashMap<>();
        this.tableFiles = new HashMap<>();
        loadSchema();
    }

    public static DatabaseCatalog getInstance(String databaseDir) {
        if (instance == null) {
            instance = new DatabaseCatalog(databaseDir);
        }
        return instance;
    }

    /**
     * Loads the schema.txt file to initialize table metadata.
     */
    private void loadSchema() {
        File schemaFile = new File(databaseDir + File.separator + "schema.txt");

        try (BufferedReader reader = new BufferedReader(new FileReader(schemaFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\s+"); // Split by spaces
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
     * Gets the file for a given table.
     * @param tableName The name of the table.
     * @return The File object representing the table's CSV file.
     */
    public File getTableFile(String tableName) {
        return tableFiles.get(tableName);
    }

    /**
     * Gets the schema (column names) of a table.
     * @param tableName The name of the table.
     * @return A list of column names.
     */
    public List<String> getTableSchema(String tableName) {
        return tableSchemas.get(tableName);
    }

    /**
     * Prints the metadata of the database catalog.
     */
    public void printCatalog(Boolean showFilePath) {
        System.out.println("Database Directory: " + databaseDir);
        System.out.println("Tables:");
        for (String tableName : tableSchemas.keySet()) {
            System.out.println("- Table Name: " + tableName);
            System.out.println("  Columns:\t  " + String.join(", ", tableSchemas.get(tableName)));
            if (showFilePath) System.out.println("  File Path: " + tableFiles.get(tableName).getAbsolutePath());
        }
    }
}