package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.utility.Parser;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.expression.Expression;

import java.util.*;

import static ed.inf.adbs.blazedb.Helper.getIndices;

/**
 * ProjectOperator extracts only specific columns from tuples.
 */
public class ProjectOperator extends Operator {
    private final Operator childOperator;
    private final List<SelectItem<?>> selectItems;
    private final List<Integer> selectedColumnIndexes;

    /**
     * Initializes the ProjectOperator for the given child operator and parser.
     * @param childOperator The operator producing the tuples to project.
     * @param parser The parser containing the select items and table order.
     */
    public ProjectOperator(Operator childOperator, Parser parser) {
        this.childOperator = childOperator;
        this.selectItems = parser.getSelectItems();

        if (isSelectAll()) {
            this.selectedColumnIndexes = null;  // NULL means return full tuple
        } else {
            this.selectedColumnIndexes = new ArrayList<>();
            for (SelectItem<?> selectItem : selectItems) {
                Expression expression = selectItem.getExpression();
                this.selectedColumnIndexes.addAll(getIndices(expression, parser.getTableOrder()));
            }
        }
    }

    /**
     * Retrieves the next tuple from the child operator with only the selected columns.
     * @return The projected tuple or null if there are no more tuples.
     */
    @Override
    public Tuple getNextTuple() {
        Tuple tuple = childOperator.getNextTuple();
        if (tuple == null) return null;

        if (isSelectAll()) {
            return tuple;  // Return full tuple
        } else {
            List<String> projectedValues = new ArrayList<>();
            for (Integer selectedColumnIndex : selectedColumnIndexes) {
                String value = tuple.getValue(selectedColumnIndex).replaceAll("\\s", "");
                projectedValues.add(value);
            }
            return new Tuple(projectedValues.toArray(new String[0]));
        }
    }

    /**
     * Resets the child operator for the next iteration.
     */
    @Override
    public void reset() {
        childOperator.reset();
    }

    /**
     * Determines whether the SELECT clause is requesting all columns.
     * @return True if selecting all columns, false otherwise.
     */
    private boolean isSelectAll() {
        return selectItems.size() == 1 && Objects.equals(selectItems.get(0).toString(), "*");
    }
}
