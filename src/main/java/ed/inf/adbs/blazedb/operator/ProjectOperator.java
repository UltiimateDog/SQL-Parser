package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DatabaseCatalog;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.expression.Expression;

import java.util.*;


/**
 * ProjectOperator extracts only specific columns from tuples.
 */
public class ProjectOperator extends Operator {
    private final Operator childOperator;
    private final List<SelectItem<?>> selectItems;
    private final Map<String, List<String>> fullSchema;
    private final List<Integer> selectedColumnIndexes;

    public ProjectOperator(Operator childOperator, List<SelectItem<?>> selectItems) {
        this.childOperator = childOperator;
        this.selectItems = selectItems;
        this.fullSchema = DatabaseCatalog.getInstance().getTableSchemas();

        // Determine which columns we need from the full schema
        if (isSelectAll()) {
            this.selectedColumnIndexes = null;  // NULL means return full tuple
        } else {
            this.selectedColumnIndexes = new ArrayList<>();
            for (SelectItem<?> selectItem : selectItems) {
                Expression expression = selectItem.getExpression();
                Column column = (Column) expression;
                String columnName = column.getColumnName();
                String tableName = column.getTable().getName();

                if (!fullSchema.containsKey(tableName)) {
                    throw new IllegalArgumentException("Table not found in schema: " + tableName);
                }

                List<String> schema = fullSchema.get(tableName);
                int columnIndex = schema.indexOf(columnName);

                if (columnIndex == -1) {
                    throw new IllegalArgumentException("Column not found in schema: " + columnName);
                }
                this.selectedColumnIndexes.add(columnIndex);
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
