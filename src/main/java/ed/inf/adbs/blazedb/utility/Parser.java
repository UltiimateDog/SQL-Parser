package ed.inf.adbs.blazedb.utility;

import ed.inf.adbs.blazedb.Tuple;
import lombok.Getter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.expression.Expression;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

@Getter
public class Parser {
    private String parsedSQL; // Stores the original SQL query as a string
    private List<SelectItem<?>> selectItems; // Stores the SELECT items (columns, expressions, etc.)
    private List<String> selectColumns; // Stores column names that are explicitly selected (excluding SUM)
    private List<String> sumColumns; // Stores column names used in SUM() aggregation
    private FromItem fromTable; // Stores the main table from the FROM clause
    private List<Join> joins; // Stores the JOIN clauses
    private Expression whereClause; // Stores the WHERE condition as an Expression object
    private List<String> nonJoinConditions; // Stores conditions in WHERE that are not related to JOINs
    private List<OrderByElement> orderByElements; // Stores ORDER BY elements
    private List<String> groupByColumns; // Stores GROUP BY column names
    private List<String> tableOrder; // Stores the order of tables as they appear in FROM and JOINs
    private Boolean isDistinct; // Indicates whether the DISTINCT keyword is present in the query

    public Parser(String filename) {
        try {
            // Parse the SQL query from the file using JSQLParser
            Statement statement = CCJSqlParserUtil.parse(new FileReader(filename));
            if (statement instanceof Select) {
                Select select = (Select) statement;
                PlainSelect plainSelect = (PlainSelect) select.getPlainSelect(); // Extract the main query structure

                this.parsedSQL = select.toString(); // Store the parsed SQL string
                this.selectItems = plainSelect.getSelectItems(); // Retrieve the SELECT clause elements
                this.fromTable = plainSelect.getFromItem(); // Extract the main table from the FROM clause
                this.joins = plainSelect.getJoins(); // Extract JOIN statements
                this.whereClause = plainSelect.getWhere(); // Extract the WHERE clause condition
                this.orderByElements = plainSelect.getOrderByElements(); // Extract ORDER BY clause if present
                this.groupByColumns = plainSelect.getGroupBy() != null ?
                        List.of(plainSelect.getGroupBy().getGroupByExpressionList().toString().replaceAll("\\s*", "").split(","))
                        : null; // Extract GROUP BY columns safely
                this.isDistinct = plainSelect.getDistinct() != null; // Check if DISTINCT is present

                this.selectColumns = new ArrayList<>();
                this.sumColumns = new ArrayList<>();

                // Extract SUM expressions separately from other SELECT columns
                separateSumExpressions();

                // Extract table names from FROM and JOIN clauses
                extractTableNames();

                // Identify tables used in WHERE conditions (excluding JOIN conditions)
                extractTablesFromWhereClause();
            }
        } catch (Exception e) {
            System.err.println("Exception occurred during parsing");
            e.printStackTrace();
        }
    }

    /**
     * Extracts table names from the FROM clause and JOIN clauses.
     */
    private void extractTableNames() {
        tableOrder = new ArrayList<>();
        if (fromTable != null) {
            tableOrder.add(fromTable.toString());
        }
        if (joins != null) {
            for (Join join : joins) {
                tableOrder.add(join.getRightItem().toString());
            }
        }
    }

    /**
     * Separates SUM(expression) from normal column expressions in selectItems.
     * It identifies whether a SELECT item contains a SUM() function and stores it separately.
     */
    private void separateSumExpressions() {
        for (SelectItem<?> item : selectItems) {
            Expression expression = item.getExpression();
            if (expression instanceof Function) { // Check if the item is a function
                Function function = (Function) expression;
                if ("SUM".equalsIgnoreCase(function.getName())) { // Check if it's a SUM function
                    sumColumns.add(item.toString()); // Add to sumColumns list
                    continue; // Skip adding to selectColumns
                }
            }
            selectColumns.add(item.toString()); // Add to normal column selection
        }
    }

    /**
     * Extracts table names referenced in the WHERE clause that are not part of a JOIN condition.
     */
    private void extractTablesFromWhereClause() {
        nonJoinConditions = new ArrayList<>();

        // Create an instance of ExpressionEvaluator to analyze the WHERE clause
        ExpressionEvaluator evaluator = new ExpressionEvaluator(
                List.of(new String[] { "" }), // Empty dummy input
                new Tuple(new String[] { "" }) // Empty tuple input
        );

        if (whereClause != null) {
            evaluator.evaluate(whereClause); // Evaluate WHERE conditions
            for (String expression : evaluator.getTablesForExpression()) {
                if (!expression.contains(",")) { // Ensure it's a single table reference
                    nonJoinConditions.add(expression); // Add to non-join conditions
                }
            }
        }
    }

    /**
     * Prints the parsed SQL components for debugging purposes.
     */
    public void printExpression() {
        // Print parsed SQL statement components in a structured format
        System.out.println("Parsed SQL Statement: " + parsedSQL + "\n");
        System.out.println("SELECT:\t\t " + selectColumns); // List of selected columns
        System.out.println("SUM:\t\t " + sumColumns); // List of SUM() columns
        System.out.println("DISTINCT:\t " + (isDistinct ? "true" : "")); // DISTINCT status
        System.out.println("FROM:\t\t " + tableOrder); // List of tables involved
        System.out.println("WHERE:\t\t " + (whereClause != null ? whereClause : "") ); // WHERE clause details
        System.out.println("ORDER BY:\t " + (orderByElements != null ? orderByElements : "")); // ORDER BY details
        System.out.println("GROUP BY:\t " + (groupByColumns != null ? groupByColumns : "")); // GROUP BY details
        System.out.println("T:\t\t " + nonJoinConditions); // Non-join conditions in WHERE
    }
}
