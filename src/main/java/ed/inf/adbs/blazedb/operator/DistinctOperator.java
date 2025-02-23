package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;

import java.util.HashSet;
import java.util.Set;

/**
 * DistinctOperator removes duplicate tuples from query results.
 */
public class DistinctOperator extends Operator {
    private final Operator childOperator;
    private final Set<String> seenTuples;
    private Tuple nextTuple;

    public DistinctOperator(Operator childOperator) {
        this.childOperator = childOperator;
        this.seenTuples = new HashSet<>();
        this.nextTuple = fetchNextUniqueTuple();
    }

    /**
     * Fetches the next unique tuple.
     */
    private Tuple fetchNextUniqueTuple() {
        Tuple tuple;
        while ((tuple = childOperator.getNextTuple()) != null) {
            String tupleStr = tuple.toString(); // Convert tuple to string for uniqueness check
            if (seenTuples.add(tupleStr)) {  // Only return if it's not a duplicate
                return tuple;
            }
        }
        return null; // No more unique tuples
    }

    @Override
    public Tuple getNextTuple() {
        Tuple current = nextTuple;
        nextTuple = fetchNextUniqueTuple(); // Preload the next unique tuple
        return current;
    }

    @Override
    public void reset() {
        childOperator.reset();
        seenTuples.clear(); // Clear memory
        nextTuple = fetchNextUniqueTuple(); // Reload distinct tuples
    }
}
