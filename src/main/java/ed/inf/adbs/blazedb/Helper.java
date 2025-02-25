package ed.inf.adbs.blazedb;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class Helper {
    public static List<List<Integer>> CSV_Parser(String filePath) throws IOException {
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

    public static boolean CSV_Equals(String filePath1, String filePath2) throws IOException {
        List<List<Integer>> data1 = CSV_Parser(filePath1);
        List<List<Integer>> data2 = CSV_Parser(filePath2);

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

    public static List<Integer> getIndices(Expression expression, List<String> tableOrder) {
        List<Integer> indices = new ArrayList<>();
        String expressionStr = expression.toString();

        // Extract table and column name from the expression
        return getIndicesList(expressionStr, tableOrder, indices);
    }

    public static List<Integer> getIndices(String expression, List<String> tableOrder) {
        List<Integer> indices = new ArrayList<>();

        // Extract table and column name from the expression
        return getIndicesList(expression, tableOrder, indices);
    }

    private static List<Integer> getIndicesList(String expression, List<String> tableOrder, List<Integer> indices) {
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

        // Calculate offset once
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

    public static List<String> getComparedTables(Tuple tuple, List<String> tableOrder) {
        List<String> comparedTables = new ArrayList<>();
        Map<String, List<String>> tableSchemas = DatabaseCatalog.getInstance().getTableSchemas(tableOrder);
        int tupleSize = tuple.getValues().size();
        int accumulatedSize = 0;

        for (String table : tableOrder) {
            int tableSize = tableSchemas.get(table).size();
            accumulatedSize += tableSize;

            if (tupleSize <= accumulatedSize) {
                comparedTables.add(table);
                break;
            }
            comparedTables.add(table);
        }

        return comparedTables;
    }
}

