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
    private final Map<String, List<String>> tableSchemas;

    public SelectOperator(Operator childOperator, Expression whereClause) {
        this.childOperator = childOperator;
        this.whereClause = whereClause;
        this.tableSchemas = DatabaseCatalog.getInstance().getTableSchemas();
    }

    @Override
    public Tuple getNextTuple() {
        Tuple tuple;
        while ((tuple = childOperator.getNextTuple()) != null) {
            ExpressionEvaluator evaluator = new ExpressionEvaluator(tableSchemas, tuple);
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
