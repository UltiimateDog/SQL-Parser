package ed.inf.adbs.blazedb.parsers;

import ed.inf.adbs.blazedb.Tuple;
import lombok.Getter;
import net.sf.jsqlparser.expression.Function;
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
    private List<String> selectColumns;
    private List<String> sumColumns;
    private FromItem fromTable;
    private List<Join> joins;
    private Expression whereClause;
    private List<String> tablesInWhere;
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

                this.selectColumns = new ArrayList<>();
                this.sumColumns = new ArrayList<>();
                separateSumExpressions();
                extractTableNames();
                extractTablesFromWhereClause();
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

    /**
     * Separates SUM(expression) from normal column expressions in selectItems.
     */
    private void separateSumExpressions() {
        for (SelectItem<?> item : selectItems) {
            Expression expression = item.getExpression();
            if (expression instanceof Function) {
                Function function = (Function) expression;
                if ("SUM".equalsIgnoreCase(function.getName())) {
                    sumColumns.add(item.toString());
                    continue;
                }
            }
            selectColumns.add(item.toString());
        }
    }

    private void extractTablesFromWhereClause() {
        ExpressionEvaluator evaluator = new ExpressionEvaluator(
                List.of(new String[] { "" }),
                new Tuple(new String[] { "" }));
        if (whereClause != null) {
            evaluator.evaluate(whereClause);
            tablesInWhere = evaluator.getTablesForExpression();
        }
    }

    public void printExpression() {
        // Print parsed SQL components
        System.out.println("Parsed SQL Statement: " + parsedSQL + "\n");
        System.out.println("SELECT:\t\t " + selectColumns);
        System.out.println("SUM:\t\t " + sumColumns);
        System.out.println("DISTINCT:\t " + (isDistinct ? "true" : ""));
        System.out.println("FROM:\t\t " + tableOrder);
        System.out.println("WHERE:\t\t " + (whereClause != null ? whereClause : "") );
        System.out.println("ORDER BY:\t " + (orderByElements != null ? orderByElements : ""));
        System.out.println("GROUP BY:\t " + (groupByColumns != null ? groupByColumns : ""));
        System.out.println("T:\t\t " + tablesInWhere);
    }
}
