package ed.inf.adbs.blazedb.parsers;

import lombok.Getter;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.expression.Expression;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

@Getter
public class Parser {
    private String parsedSQL;
    private List<SelectItem<?>> selectItems;
    private FromItem fromTable;
    private List<Join> joins;
    private Expression whereClause;
    private List<OrderByElement> orderByElements;
    private List<String> groupByColumns;
    private List<String> tableOrder;
    private Boolean isDistinct;

    public Parser(String filename) {
        try {
            Statement statement = CCJSqlParserUtil.parse(new FileReader(filename));
            if (statement instanceof Select) {
                Select select = (Select) statement;
                PlainSelect plainSelect = (PlainSelect) select.getPlainSelect();

                this.parsedSQL = select.toString();
                this.selectItems = plainSelect.getSelectItems();
                this.fromTable = plainSelect.getFromItem();
                this.joins = plainSelect.getJoins();
                this.whereClause = plainSelect.getWhere();
                this.orderByElements = plainSelect.getOrderByElements();
                this.groupByColumns = plainSelect.getGroupBy() != null ?
                        List.of(plainSelect.getGroupBy().getGroupByExpressionList().toString().replaceAll("\\s*", "").split(","))
                        : null;
                this.isDistinct = plainSelect.getDistinct() != null;

                extractTableNames();
            }
        } catch (Exception e) {
            System.err.println("Exception occurred during parsing");
            e.printStackTrace();
        }
    }

    /**
     * Extracts table names from the FROM clause and JOIN clauses.
     */
    private void extractTableNames() {
        tableOrder = new ArrayList<>();
        if (fromTable != null) {
            tableOrder.add(fromTable.toString());  // Add main table
        }
        if (joins != null) {
            for (Join join : joins) {
                tableOrder.add(join.getRightItem().toString());  // Add joined tables
            }
        }
    }

    public void printExpression() {
        // Print parsed SQL components
        System.out.println("Parsed SQL Statement: " + parsedSQL + "\n");
        System.out.println("SELECT:\t\t " + selectItems);
        System.out.println("DISTINCT:\t " + (isDistinct ? "true" : ""));
        System.out.println("FROM:\t\t " + tableOrder);
        System.out.println("WHERE:\t\t " + (whereClause != null ? whereClause : "") );
        System.out.println("ORDER BY:\t " + (orderByElements != null ? orderByElements : ""));
        System.out.println("GROUP BY:\t " + (groupByColumns != null ? groupByColumns : ""));
    }
}
