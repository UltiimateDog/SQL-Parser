package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.utility.ExpressionEvaluator;
import ed.inf.adbs.blazedb.utility.Parser;

import java.util.*;

import static ed.inf.adbs.blazedb.Helper.extractSumExpression;
import static ed.inf.adbs.blazedb.Helper.getIndices;

/**
 * SumOperator handles GROUP BY and SUM aggregation for multiple sum columns.
 */
public class SumOperator extends Operator {
    private final Operator childOperator;
    private final List<String> groupByColumns;
    private final List<String> sumColumns;
    private final List<String> selectColumns;
    private final Map<List<String>, List<Integer>> groupSumMap; // Group key → List of SUM results
    private Iterator<Map.Entry<List<String>, List<Integer>>> iterator;
    private final List<String> tableOrder; // Stores the table processing order

    public SumOperator(Operator childOperator, Parser parser) {
        this.childOperator = childOperator;
        this.groupByColumns = parser.getGroupByColumns();
        this.tableOrder = parser.getTableOrder();
        this.sumColumns = parser.getSumColumns();
        this.selectColumns = parser.getSelectColumns();
        this.groupSumMap = new LinkedHashMap<>();
        aggregateTuples(); // Process and compute SUM
        this.iterator = groupSumMap.entrySet().iterator();
    }

    /**
     * Reads all tuples, groups them, and computes SUM for multiple columns.
     */
    private void aggregateTuples() {
        Tuple tuple;
        while ((tuple = childOperator.getNextTuple()) != null) {
            List<String> groupKey = extractGroupKey(tuple);
            ExpressionEvaluator evaluator = new ExpressionEvaluator(tableOrder, tuple);

            // Compute sum values for all sum columns
            List<Integer> sumValues = new ArrayList<>();
            for (String sumColumn : sumColumns) {
                sumValues.add(evaluator.evaluateSumExpression(extractSumExpression(sumColumn)));
            }

            // Update the group sum map
            groupSumMap.merge(groupKey, sumValues, (existingSums, newSums) -> {
                for (int i = 0; i < existingSums.size(); i++) {
                    existingSums.set(i, existingSums.get(i) + newSums.get(i));
                }
                return existingSums;
            });
        }
    }

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

        if (sumColumns.isEmpty()) {
            return new Tuple(resultValues.toArray(new String[0]));
        }

        entry.getValue().forEach(sum -> resultValues.add(String.valueOf(sum))); // Add SUM results

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
