package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.utility.Parser;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static ed.inf.adbs.blazedb.Helper.getIndices;

/**
 * SortOperator sorts tuples based on ORDER BY columns.
 */
public class SortOperator extends Operator {
    private final Operator childOperator;
    private final List<OrderByElement> orderByElements;
    private final List<Tuple> sortedTuples;
    private final List<String> tableOrder; // Stores the table processing order
    private int currentIndex;

    public SortOperator(Operator childOperator, Parser parser) {
        this.childOperator = childOperator;
        this.orderByElements = parser.getOrderByElements();
        this.tableOrder = parser.getTableOrder();
        this.sortedTuples = new ArrayList<>();
        this.currentIndex = 0;
        loadAndSortTuples(); // Sort tuples on initialization
    }

    /**
     * Loads tuples from the child operator and sorts them.
     */
    private void loadAndSortTuples() {
        Tuple tuple;
        while ((tuple = childOperator.getNextTuple()) != null) {
            sortedTuples.add(tuple);
        }

        // Sort tuples based on ORDER BY columns
        if (!orderByElements.isEmpty()) {
            sortedTuples.sort(new TupleComparator(orderByElements, tableOrder));
        }
    }

    @Override
    public Tuple getNextTuple() {
        return (currentIndex < sortedTuples.size()) ? sortedTuples.get(currentIndex++) : null;
    }

    @Override
    public void reset() {
        currentIndex = 0;
    }

    /**
     * Custom comparator for sorting tuples based on ORDER BY.
     */
    private static class TupleComparator implements Comparator<Tuple> {
        private final List<OrderByElement> orderByElements;
        private final List<String> tableOrder;

        public TupleComparator(List<OrderByElement> orderByElements, List<String> tableOrder) {
            this.orderByElements = orderByElements;
            this.tableOrder = tableOrder;
        }

        @Override
        public int compare(Tuple t1, Tuple t2) {
            for (OrderByElement orderBy : orderByElements) {
                Expression expression = orderBy.getExpression(); // Extract ORDER BY column expression
                List<Integer> indices = getIndices(expression, tableOrder); // Get column indices

                if (indices.isEmpty() || indices.get(0) == -2) {
                    throw new IllegalArgumentException("Invalid ORDER BY column: " + expression);
                }

                int index = indices.get(0); // ORDER BY always works on a single column
                int val1 = Integer.parseInt(t1.getValue(index));
                int val2 = Integer.parseInt(t2.getValue(index));

                int comparison = Integer.compare(val1, val2);
                if (comparison != 0) return comparison; // Return first non-zero comparison
            }
            return 0; // All ORDER BY columns are equal
        }
    }
}
