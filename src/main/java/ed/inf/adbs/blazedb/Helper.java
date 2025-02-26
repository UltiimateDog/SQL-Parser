package ed.inf.adbs.blazedb;

import net.sf.jsqlparser.expression.Expression;
import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Helper class providing utility methods for CSV parsing, schema indexing, and expression extraction.
 * Includes methods for parsing CSV files, comparing CSV contents, retrieving column indices,
 * identifying compared tables, and extracting expressions from SQL queries.
 */
public class Helper {

    /**
     * Parses a CSV file into a list of integer lists.
     *
     * @param filePath The path to the CSV file.
     * @return A list of lists containing integer values parsed from the CSV file.
     * @throws IOException If there is an error reading the file.
     */
    public static List<List<Integer>> parseCSV(String filePath) throws IOException {
        List<List<Integer>> data = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                List<Integer> row = Arrays.stream(line.split(",\\s*")) // Split by comma, ignoring spaces
                        .map(Integer::parseInt)   // Convert each value to an Integer
                        .collect(Collectors.toList());
                data.add(row);
            }
        }
        return data;
    }

    /**
     * Compares two CSV files to check if they are equal in terms of data content.
     *
     * @param filePath1 The path to the first CSV file.
     * @param filePath2 The path to the second CSV file.
     * @return True if both CSV files contain identical data; false otherwise.
     * @throws IOException If there is an error reading the files.
     */
    public static boolean csvEquals(String filePath1, String filePath2) throws IOException {
        List<List<Integer>> data1 = parseCSV(filePath1);
        List<List<Integer>> data2 = parseCSV(filePath2);

        if (data1.size() != data2.size()) {
            return false;
        }

        for (int i = 0; i < data1.size(); i++) {
            if (!data1.get(i).equals(data2.get(i))) { // Compare lists of integers
                return false;
            }
        }

        return true;
    }

    /**
     * Retrieves the column indices based on an SQL expression.
     *
     * @param expression   The SQL expression.
     * @param tableOrder   The list of tables in join order.
     * @return A list of column indices corresponding to the expression.
     */
    public static List<Integer> getIndices(Expression expression, List<String> tableOrder) {
        return getIndices(expression.toString(), tableOrder);
    }

    /**
     * Retrieves the column indices based on an SQL expression in string format.
     *
     * @param expression   The string representation of an SQL expression.
     * @param tableOrder   The list of tables in join order.
     * @return A list of column indices corresponding to the expression.
     * @throws IllegalArgumentException If the expression format is invalid or table/column is not found.
     */
    public static List<Integer> getIndices(String expression, List<String> tableOrder) {
        List<Integer> indices = new ArrayList<>();
        return resolveIndices(expression, tableOrder, indices);
    }

    /**
     * Resolves column indices based on a given expression and table order.
     *
     * @param expression   The column expression (e.g., "table.column").
     * @param tableOrder   The list of tables in join order.
     * @param indices      The list of resolved indices.
     * @return A list of column indices corresponding to the expression.
     * @throws IllegalArgumentException If the column or table is not found.
     */
    private static List<Integer> resolveIndices(String expression,
                                                List<String> tableOrder,
                                                List<Integer> indices) {
        String[] parts = expression.split("\\.");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid column expression: " + expression);
        }

        String tableName = parts[0];
        String columnName = parts[1];
        Map<String, List<String>> tableSchemas = DatabaseCatalog.getInstance().getTableSchemas(tableOrder);

        if (!DatabaseCatalog.getInstance().getTableSchemas().containsKey(tableName)) {
            throw new IllegalArgumentException("Table not found in schema: " + tableName);
        }

        if (!tableSchemas.containsKey(tableName)) {
            indices.add(-2);
            return indices;
        }

        List<String> schema = tableSchemas.get(tableName);

        int offset = 0;
        for (int i = 0; i < tableOrder.indexOf(tableName); i++) {
            offset += tableSchemas.get(tableOrder.get(i)).size();
        }

        if (columnName.equals("*")) {
            for (int i = 0; i < schema.size(); i++) {
                indices.add(offset + i);
            }
        } else {
            int columnIndex = schema.indexOf(columnName);
            if (columnIndex == -1) {
                throw new IllegalArgumentException("Column not found in schema: " + columnName);
            }
            indices.add(offset + columnIndex);
        }

        return indices;
    }

    /**
     * Extracts the expression inside a SUM() function from an SQL query.
     *
     * @param input The SQL expression containing SUM().
     * @return The extracted expression inside SUM(), or null if not found.
     */
    public static String extractSumExpression(String input) {
        Pattern pattern = Pattern.compile("SUM\\((.*?)\\)");
        Matcher matcher = pattern.matcher(input);
        return matcher.find() ? matcher.group(1) : null;
    }
}
