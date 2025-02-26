package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.utility.Parser;
import net.sf.jsqlparser.expression.Expression;
import ed.inf.adbs.blazedb.utility.ExpressionEvaluator;

import java.util.List;

/**
 * SelectOperator filters tuples based on WHERE conditions.
 */
public class SelectOperator extends Operator {
    private final Operator childOperator;
    private final Expression whereClause;
    private final List<String> tableOrder;

    /**
     * Initializes the SelectOperator with child operator, parser, and table order.
     * @param childOperator The child operator to retrieve tuples from.
     * @param parser The parser containing WHERE clause.
     * @param tableOrder The order of the tables.
     */
    public SelectOperator(Operator childOperator, Parser parser, List<String> tableOrder) {
        this.childOperator = childOperator;
        this.whereClause = parser.getWhereClause();
        this.tableOrder = tableOrder;
    }

    /**
     * Retrieves the next tuple that satisfies the WHERE clause condition.
     * @return The next tuple that satisfies the WHERE condition or null if no more tuples.
     */
    @Override
    public Tuple getNextTuple() {
        Tuple tuple;
        while ((tuple = childOperator.getNextTuple()) != null) {
            ExpressionEvaluator evaluator = new ExpressionEvaluator(tableOrder, tuple);
            if (whereClause == null || evaluator.evaluate(whereClause)) {
                return tuple;
            }
        }
        return null;
    }

    /**
     * Resets the child operator to the beginning.
     */
    @Override
    public void reset() {
        childOperator.reset();
    }
}
