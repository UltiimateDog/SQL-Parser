package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.parsers.ExpressionEvaluator;
import net.sf.jsqlparser.expression.Expression;


import java.util.*;

import static ed.inf.adbs.blazedb.Helper.extractSumExpression;
import static ed.inf.adbs.blazedb.Helper.getIndices;

/**
 * SumOperator handles GROUP BY and SUM aggregation.
 */
public class SumOperator extends Operator {
    private final Operator childOperator;
    private final List<String> groupByColumns;
    private final String sumExpression;
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
        this.sumExpression = sumExpression.toString();
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
            ExpressionEvaluator evaluator = new ExpressionEvaluator(tableOrder, tuple);
            int sumValue = evaluator.evaluateSumExpression(extractSumExpression(sumExpression));

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
            int index = getIndices(column, tableOrder).get(0);
            key.add(tuple.getValue(index));
        }
        return key;
    }
}
