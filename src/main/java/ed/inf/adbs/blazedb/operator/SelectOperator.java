package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DatabaseCatalog;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import ed.inf.adbs.blazedb.parsers.ExpressionEvaluator;

import java.util.List;
import java.util.Map;

/**
 * SelectOperator filters tuples based on WHERE conditions.
 */
public class SelectOperator extends Operator {
    private final Operator childOperator;
    private final Expression whereClause;
    private final List<String> tableOrder;

    public SelectOperator(Operator childOperator, Expression whereClause, List<String> tableOrder) {
        this.childOperator = childOperator;
        this.whereClause = whereClause;
        this.tableOrder = tableOrder;
    }

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

    @Override
    public void reset() {
        childOperator.reset();
    }
}
