package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DatabaseCatalog;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.schema.Column;
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

    public ProjectOperator(Operator childOperator, List<SelectItem<?>> selectItems, List<String> tableOrder) {
        this.childOperator = childOperator;
        this.selectItems = selectItems;

        // Determine which columns we need from the full schema
        if (isSelectAll()) {
            this.selectedColumnIndexes = null;  // NULL means return full tuple
        } else {
            this.selectedColumnIndexes = new ArrayList<>();
            for (SelectItem<?> selectItem : selectItems) {
                Expression expression = selectItem.getExpression();
                this.selectedColumnIndexes.addAll(getIndices(expression, tableOrder));
            }
        }
    }

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

    @Override
    public void reset() {
        childOperator.reset();
    }

    private boolean isSelectAll() {
        return selectItems.size() == 1 && Objects.equals(selectItems.get(0).toString(), "*");
    }
}
