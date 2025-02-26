package ed.inf.adbs.blazedb.utility;

import ed.inf.adbs.blazedb.operator.*;

import java.io.IOException;
import java.util.List;

/**
 * The Planner class constructs a query execution plan from a parsed SQL query.
 * It builds an operator tree, starting with scan operations and progressively
 * applying selection, joins, aggregation, projection, sorting, and distinct operators.
 */
public class Planner {
    private final Parser parser;

    /**
     * Constructs a Planner with a given SQL parser.
     *
     * @param parser The parsed SQL query representation.
     */
    public Planner(Parser parser) {
        this.parser = parser;
    }

    /**
     * Constructs and returns the query execution plan as an operator tree.
     *
     * @return The root {@code Operator} of the query plan.
     * @throws IOException If there is an error accessing the database files.
     */
    public Operator buildQueryPlan() throws IOException {
        Operator rootOperator = createBaseOperators();

        // ✅ Apply ORDER BY if present
        if (parser.getOrderByElements() != null) {
            rootOperator = new SortOperator(rootOperator, parser);
        }

        // ✅ Apply GROUP BY + SUM, or fallback to Projection
        if (parser.getGroupByColumns() != null || !parser.getSumColumns().isEmpty()) {
            rootOperator = new SumOperator(rootOperator, parser);
        } else {
            rootOperator = new ProjectOperator(rootOperator, parser);
        }

        // ✅ Apply DISTINCT if required
        if (parser.getIsDistinct()) {
            rootOperator = new DistinctOperator(rootOperator);
        }

        return rootOperator;
    }

    /**
     * Creates the base operators for the query plan, including scanning,
     * filtering (selection), and joins.
     *
     * @return The root {@code Operator} for the base query structure.
     * @throws IOException If an error occurs while accessing database files.
     */
    private Operator createBaseOperators() throws IOException {
        List<String> tableOrder = parser.getTableOrder();

        if (tableOrder.isEmpty()) {
            throw new IllegalStateException("No tables found in query.");
        }

        // ✅ Create ScanOperator for the main table
        String mainTable = tableOrder.get(0);
        Operator rootOperator = new ScanOperator(mainTable);

        // ✅ Apply Selection (WHERE conditions) for the main table if applicable
        if (parser.getNonJoinConditions().contains(mainTable)) {
            rootOperator = new SelectOperator(rootOperator, parser, List.of(mainTable));
        }

        // ✅ Apply Joins if there are multiple tables in the query
        for (int i = 1; i < tableOrder.size(); i++) {
            String joinTable = tableOrder.get(i);
            Operator joinScan = new ScanOperator(joinTable);

            // ✅ Apply Selection for the joined table before performing the join
            if (parser.getNonJoinConditions().contains(joinTable)) {
                joinScan = new SelectOperator(joinScan, parser, List.of(joinTable));
            }

            rootOperator = new JoinOperator(rootOperator, joinScan, parser);
        }

        return rootOperator;
    }
}
