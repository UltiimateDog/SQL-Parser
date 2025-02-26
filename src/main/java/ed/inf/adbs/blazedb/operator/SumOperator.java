package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.utility.ExpressionEvaluator;
import ed.inf.adbs.blazedb.utility.Parser;

import java.util.*;

import static ed.inf.adbs.blazedb.Helper.extractSumExpression;
import static ed.inf.adbs.blazedb.Helper.getIndices;

/**
 * SumOperator handles GROUP BY and SUM aggregation for multiple sum columns.
 * It processes tuples, groups them by specified columns, and computes the SUM for each group.
 */
public class SumOperator extends Operator {
    private final Operator childOperator;
    private final List<String> groupByColumns;
    private final List<String> sumColumns;
    private final List<String> selectColumns;
    private final Map<List<String>, List<Integer>> groupSumMap;
    private Iterator<Map.Entry<List<String>, List<Integer>>> iterator;
    private final List<String> tableOrder;

    /**
     * Initializes SumOperator with the child operator and parser information.
     * @param childOperator The child operator to retrieve tuples from.
     * @param parser The parser to extract GROUP BY, SUM, and SELECT columns.
     */
    public SumOperator(Operator childOperator, Parser parser) {
        this.childOperator = childOperator;
        this.groupByColumns = parser.getGroupByColumns();
        this.tableOrder = parser.getTableOrder();
        this.sumColumns = parser.getSumColumns();
        this.selectColumns = parser.getSelectColumns();
        this.groupSumMap = new LinkedHashMap<>();
        aggregateTuples();
        this.iterator = groupSumMap.entrySet().iterator();
    }

    /**
     * Groups the tuples and computes SUM for the specified columns.
     */
    private void aggregateTuples() {
        Tuple tuple;
        while ((tuple = childOperator.getNextTuple()) != null) {
            List<String> groupKey = extractGroupKey(tuple);
            ExpressionEvaluator evaluator = new ExpressionEvaluator(tableOrder, tuple);

            List<Integer> sumValues = new ArrayList<>();
            for (String sumColumn : sumColumns) {
                sumValues.add(evaluator.evaluateSumExpression(extractSumExpression(sumColumn)));
            }

            groupSumMap.merge(groupKey, sumValues, (existingSums, newSums) -> {
                for (int i = 0; i < existingSums.size(); i++) {
                    existingSums.set(i, existingSums.get(i) + newSums.get(i));
                }
                return existingSums;
            });
        }
    }

    /**
     * Returns the next tuple from the grouped results.
     * @return The next tuple with group values and computed SUM values.
     */
    @Override
    public Tuple getNextTuple() {
        if (!iterator.hasNext()) return null;

        Map.Entry<List<String>, List<Integer>> entry = iterator.next();
        List<String> resultValues = new ArrayList<>();

        if (!selectColumns.isEmpty()) {
            for (String column : selectColumns) {
                int index = groupByColumns.indexOf(column);
                resultValues.add(entry.getKey().get(index));
            }
        }

        if (!sumColumns.isEmpty()) {
            entry.getValue().forEach(sum -> resultValues.add(String.valueOf(sum)));
        }

        return new Tuple(resultValues.toArray(new String[0]));
    }

    /**
     * Resets the iterator to start from the beginning of the grouped results.
     */
    @Override
    public void reset() {
        iterator = groupSumMap.entrySet().iterator();
    }

    /**
     * Extracts the group key from a tuple based on the GROUP BY columns.
     * @param tuple The tuple from which the group key is extracted.
     * @return A list of values corresponding to the GROUP BY columns.
     */
    private List<String> extractGroupKey(Tuple tuple) {
        List<String> key = new ArrayList<>();
        if (groupByColumns == null) {
            key.add("");
            return key;
        }
        for (String column : groupByColumns) {
            int index = getIndices(column, tableOrder).get(0);
            key.add(tuple.getValue(index));
        }
        return key;
    }
}
