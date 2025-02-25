package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.*;

/**
 * SumOperator handles GROUP BY and SUM aggregation.
 */
public class SumOperator extends Operator {
    private final Operator childOperator;
    private final List<String> groupByColumns;
    private final Expression sumExpression;
    private final Map<List<String>, Integer> groupSumMap; // Group key → SUM result
    private Iterator<Map.Entry<List<String>, Integer>> iterator;
    private final List<String> tableOrder; // Stores the table processing order

    public SumOperator(Operator childOperator,
                       List<String> groupByColumns,
                       Expression sumExpression,
                       List<String> tableOrder) {
        this.childOperator = childOperator;
        this.groupByColumns = groupByColumns;
        this.tableOrder = tableOrder;
        this.sumExpression = sumExpression;
        this.groupSumMap = new LinkedHashMap<>();
        aggregateTuples(); // Process and compute SUM
        this.iterator = groupSumMap.entrySet().iterator();
    }

    /**
     * Reads all tuples, groups them, and computes SUM.
     */
    private void aggregateTuples() {
        Tuple tuple;
        while ((tuple = childOperator.getNextTuple()) != null) {
            List<String> groupKey = extractGroupKey(tuple);
            int sumValue = evaluateSumExpression(tuple);

            groupSumMap.put(groupKey, groupSumMap.getOrDefault(groupKey, 0) + sumValue);
        }
    }

    @Override
    public Tuple getNextTuple() {
        if (!iterator.hasNext()) return null;

        Map.Entry<List<String>, Integer> entry = iterator.next();
        List<String> resultValues = new ArrayList<>(entry.getKey());
        resultValues.add(String.valueOf(entry.getValue())); // Add SUM result

        return new Tuple(resultValues.toArray(new String[0]));
    }

    @Override
    public void reset() {
        iterator = groupSumMap.entrySet().iterator();
    }

    /**
     * Extracts the GROUP BY column values as a key.
     */
    private List<String> extractGroupKey(Tuple tuple) {
        List<String> key = new ArrayList<>();
        for (String column : groupByColumns) {
            System.out.println(column);
            int index = tuple.getValues().indexOf(column);
            key.add(tuple.getValue(index));
        }
        return key;
    }

    /**
     * Evaluates SUM expression (handles column values and multiplications).
     */
    private int evaluateSumExpression(Tuple tuple) {
        if (sumExpression instanceof Column) {
            String columnName = ((Column) sumExpression).getFullyQualifiedName();
            int index = tuple.getValues().indexOf(columnName);
            return Integer.parseInt(tuple.getValue(index));
        } else if (sumExpression instanceof Multiplication) {
            // Handle SUM(A * B)
            Multiplication multiplication = (Multiplication) sumExpression;
            int leftValue = evaluateSumExpression(tuple, multiplication.getLeftExpression());
            int rightValue = evaluateSumExpression(tuple, multiplication.getRightExpression());
            return leftValue * rightValue;
        }
        throw new IllegalArgumentException("Unsupported SUM expression: " + sumExpression);
    }

    private int evaluateSumExpression(Tuple tuple, Expression expression) {
        if (expression instanceof Column) {
            String columnName = ((Column) expression).getFullyQualifiedName();
            int index = tuple.getValues().indexOf(columnName);
            return Integer.parseInt(tuple.getValue(index));
        } else {
            throw new IllegalArgumentException("Unsupported SUM term: " + expression);
        }
    }
}
