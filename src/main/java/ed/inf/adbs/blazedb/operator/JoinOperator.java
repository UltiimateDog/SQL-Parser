package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.utility.ExpressionEvaluator;
import ed.inf.adbs.blazedb.utility.Parser;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.List;

/**
 * JoinOperator performs a nested loop join between two tables.
 */
public class JoinOperator extends Operator {
    private final Operator leftChild;
    private final Operator rightChild;
    private final Expression joinCondition;
    private final List<String> tableOrder;
    private Tuple leftTuple;

    public JoinOperator(Operator leftChild,
                        Operator rightChild,
                        Parser parser) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.joinCondition = parser.getWhereClause();
        this.tableOrder = parser.getTableOrder();
        this.leftTuple = leftChild.getNextTuple();  // Start with first left tuple
    }

    @Override
    public Tuple getNextTuple() {
        Tuple rightTuple;

        while (leftTuple != null) { // Loop over left table
            while ((rightTuple = rightChild.getNextTuple()) != null) { // Loop over right table
                List<String> combinedValues = new ArrayList<>(leftTuple.getValues());
                combinedValues.addAll(rightTuple.getValues());

                Tuple joinedTuple = new Tuple(combinedValues.toArray(new String[0]));

                // Use ExpressionEvaluator with provided schema map
                if (joinCondition == null || new ExpressionEvaluator(tableOrder, joinedTuple).evaluate(joinCondition)) {
                    return joinedTuple;
                }
            }
            rightChild.reset();  // Reset right table for next left row
            leftTuple = leftChild.getNextTuple();  // Move to next row in left table
        }

        return null;  // No more tuples to join
    }

    @Override
    public void reset() {
        leftChild.reset();
        rightChild.reset();
        leftTuple = leftChild.getNextTuple();
    }
}
