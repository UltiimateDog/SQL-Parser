package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.parsers.Parser;
import net.sf.jsqlparser.expression.Expression;
import ed.inf.adbs.blazedb.parsers.ExpressionEvaluator;

import java.util.List;

/**
 * SelectOperator filters tuples based on WHERE conditions.
 */
public class SelectOperator extends Operator {
    private final Operator childOperator;
    private final Expression whereClause;
    private final List<String> tableOrder;

    public SelectOperator(Operator childOperator, Parser parser) {
        this.childOperator = childOperator;
        this.whereClause = parser.getWhereClause();
        this.tableOrder = parser.getTableOrder();
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
