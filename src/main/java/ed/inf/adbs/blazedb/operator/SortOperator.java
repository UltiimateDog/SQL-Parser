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
    private final List<String> tableOrder;
    private int currentIndex;

    /**
     * Initializes the SortOperator with child operator and parser.
     * @param childOperator The child operator to retrieve tuples from.
     * @param parser The parser to extract ORDER BY columns.
     */
    public SortOperator(Operator childOperator, Parser parser) {
        this.childOperator = childOperator;
        this.orderByElements = parser.getOrderByElements();
        this.tableOrder = parser.getTableOrder();
        this.sortedTuples = new ArrayList<>();
        this.currentIndex = 0;
        loadAndSortTuples();
    }

    /**
     * Loads tuples from the child operator and sorts them based on ORDER BY.
     */
    private void loadAndSortTuples() {
        Tuple tuple;
        while ((tuple = childOperator.getNextTuple()) != null) {
            sortedTuples.add(tuple);
        }

        if (!orderByElements.isEmpty()) {
            sortedTuples.sort(new TupleComparator(orderByElements, tableOrder));
        }
    }

    /**
     * Returns the next tuple from the sorted list.
     * @return The next tuple if available, otherwise null.
     */
    @Override
    public Tuple getNextTuple() {
        return (currentIndex < sortedTuples.size()) ? sortedTuples.get(currentIndex++) : null;
    }

    /**
     * Resets the current index to start from the beginning.
     */
    @Override
    public void reset() {
        currentIndex = 0;
    }

    /**
     * Custom comparator for sorting tuples based on ORDER BY columns.
     */
    private static class TupleComparator implements Comparator<Tuple> {
        private final List<OrderByElement> orderByElements;
        private final List<String> tableOrder;

        /**
         * Initializes the comparator with ORDER BY elements and table order.
         * @param orderByElements The ORDER BY columns.
         * @param tableOrder The order of the tables.
         */
        public TupleComparator(List<OrderByElement> orderByElements, List<String> tableOrder) {
            this.orderByElements = orderByElements;
            this.tableOrder = tableOrder;
        }

        /**
         * Compares two tuples based on the ORDER BY columns.
         * @param t1 The first tuple.
         * @param t2 The second tuple.
         * @return A negative integer, zero, or a positive integer if t1 is less than, equal to, or greater than t2.
         */
        @Override
        public int compare(Tuple t1, Tuple t2) {
            for (OrderByElement orderBy : orderByElements) {
                Expression expression = orderBy.getExpression();
                List<Integer> indices = getIndices(expression, tableOrder);

                if (indices.isEmpty() || indices.get(0) == -2) {
                    throw new IllegalArgumentException("Invalid ORDER BY column: " + expression);
                }

                int index = indices.get(0);
                int val1 = Integer.parseInt(t1.getValue(index));
                int val2 = Integer.parseInt(t2.getValue(index));

                int comparison = Integer.compare(val1, val2);
                if (comparison != 0) return comparison;
            }
            return 0;
        }
    }
}
