package ed.inf.adbs.blazedb.utility;

import ed.inf.adbs.blazedb.DatabaseCatalog;
import ed.inf.adbs.blazedb.operator.*;
import net.sf.jsqlparser.expression.Expression;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Planner constructs a query execution plan from a parsed SQL query.
 */
public class Planner {
    private final Parser parser;

    public Planner(Parser parser) {
        this.parser = parser;
    }

    /**
     * Constructs the query execution plan.
     * @return The root Operator of the query plan.
     */
    public Operator buildQueryPlan() throws IOException {
        Operator rootOperator = createBaseOperators();
        // ✅ Apply ORDER BY
        if (parser.getOrderByElements() != null) {
            rootOperator = new SortOperator(rootOperator, parser);
        }

        // ✅ Apply GROUP BY + SUM
        if (parser.getGroupByColumns() != null || !parser.getSumColumns().isEmpty()) {
            rootOperator = new SumOperator(rootOperator, parser);
        } else {
            // ✅ Apply Projection
            rootOperator = new ProjectOperator(rootOperator, parser);
        }

        // ✅ Apply DISTINCT
        if (parser.getIsDistinct()) {
            rootOperator = new DistinctOperator(rootOperator);
        }



        return rootOperator;
    }

    /**
     * Creates the base operators: Scan, Selection, and Joins.
     */
    private Operator createBaseOperators() throws IOException {
        // ✅ Create ScanOperator for the main table
        String mainTable = parser.getTableOrder().get(0);
        Operator rootOperator = new ScanOperator(mainTable);

        if (parser.getNonJoinConditions().contains(mainTable)) {
            rootOperator = new SelectOperator(rootOperator, parser, List.of(mainTable));
        }

        // ✅ Apply Joins if there are multiple tables
        List<String> tableOrder = parser.getTableOrder();
        for (int i = 1; i < tableOrder.size(); i++) {
            String joinTable = tableOrder.get(i);
            Operator joinScan = new ScanOperator(joinTable);
            if (parser.getNonJoinConditions().contains(joinTable)) {
                joinScan = new SelectOperator(joinScan, parser, List.of(joinTable));
            }
            rootOperator = new JoinOperator(rootOperator, joinScan, parser);
        }

        return rootOperator;
    }
}
