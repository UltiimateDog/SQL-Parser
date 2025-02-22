package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DatabaseCatalog;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.AllColumns;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * ProjectOperator extracts only specific columns from tuples.
 */
public class ProjectOperator extends Operator {
    private final Operator childOperator;
    private final List<SelectItem> selectItems;
    private final List<String> fullSchema;
    private final List<Integer> selectedColumnIndexes;

    public ProjectOperator(Operator childOperator, String tableName, List<SelectItem> selectItems) {
        this.childOperator = childOperator;
        this.selectItems = selectItems;
        this.fullSchema = DatabaseCatalog.getInstance().getTableSchema(tableName);

        // Determine which columns we need from the full schema
        if (isSelectAll()) {
            this.selectedColumnIndexes = null;  // NULL means return full tuple
        } else {
            this.selectedColumnIndexes = selectItems.stream()
                    .map(item -> item.getExpression())  // Extract column names
                    .map(fullSchema::indexOf)  // Get index in schema
                    .collect(Collectors.toList());
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
                String value = tuple.getValue(selectedColumnIndex);
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
