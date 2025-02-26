package ed.inf.adbs.blazedb.utility;

import ed.inf.adbs.blazedb.Tuple;
import lombok.Getter;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.List;

import static ed.inf.adbs.blazedb.Helper.getIndices;

/**
 * Evaluates WHERE clause conditions on a given Tuple.
 */
public class ExpressionEvaluator {

    private final List<String> tableOrder; // TableName -> Column Names
    private final Tuple tuple; // The tuple being evaluated
    private Boolean ignoreFlag;
    private String currentTables;
    @Getter
    private List<String> tablesForExpression = new ArrayList<>();

    public ExpressionEvaluator(List<String> tableOrder, Tuple tuple) {
        this.tableOrder = tableOrder;
        this.tuple = tuple;
    }

    public int evaluateSumExpression(String expression) {
        expression = expression.replaceAll("\\s+", " ");
        List<String> columns = List.of(expression.split("\\*"));
        int product = 1;
        for (String column : columns) {
            if (!column.contains(".")) {
                int value = Integer.parseInt(column.replaceAll("\\s",""));
                product *= value;
                continue;
            }
            int columnIndex = getIndices(column.replaceAll("\\s",""), tableOrder).get(0);
            int value = Integer.parseInt(tuple.getValue(columnIndex).replaceAll("\\s",""));
            product *= value;
        }
        return product;
    }

    /**
     * Evaluates the given SQL WHERE expression on the tuple.
     * @param expression The WHERE clause expression.
     * @return True if the tuple satisfies the condition, otherwise false.
     */
    public boolean evaluate(Expression expression) {
        if (expression instanceof AndExpression) {
            AndExpression andExpr = (AndExpression) expression;
            return evaluate(andExpr.getLeftExpression()) && evaluate(andExpr.getRightExpression());
        } else if (expression instanceof EqualsTo) {
            return evaluateComparison((BinaryExpression) expression, "=");
        } else if (expression instanceof GreaterThan) {
            return evaluateComparison((BinaryExpression) expression, ">");
        } else if (expression instanceof GreaterThanEquals) {
            return evaluateComparison((BinaryExpression) expression, ">=");
        } else if (expression instanceof MinorThan) {
            return evaluateComparison((BinaryExpression) expression, "<");
        } else if (expression instanceof MinorThanEquals) {
            return evaluateComparison((BinaryExpression) expression, "<=");
        } else if (expression instanceof NotEqualsTo) {
            return evaluateComparison((BinaryExpression) expression, "!=");
        }
        return false; // Unsupported condition
    }

    /**
     * Evaluates a binary comparison expression.
     */
    private boolean evaluateComparison(BinaryExpression expression, String operator) {
        ignoreFlag = false;
        currentTables = "";
        int leftValue = extractValue(expression.getLeftExpression());
        int rightValue = extractValue(expression.getRightExpression());

        if (!currentTables.isEmpty()) {
            tablesForExpression.add(currentTables);
        }

        if (ignoreFlag) return true;

        return switch (operator) {
            case "=" -> leftValue == rightValue;
            case "!=" -> leftValue != rightValue;
            case ">" -> leftValue > rightValue;
            case ">=" -> leftValue >= rightValue;
            case "<" -> leftValue < rightValue;
            case "<=" -> leftValue <= rightValue;
            default -> false;
        };
    }

    /**
     * Extracts the integer value from an expression.
     */
    private int extractValue(Expression expression) {
        if (expression instanceof LongValue) {
            return (int) ((LongValue) expression).getValue();
        } else if (expression instanceof Column) {
            int columnIndex = getIndices(expression, tableOrder).get(0);

            if (!currentTables.isEmpty()) {
                currentTables += ",";
            }
            currentTables += expression.toString().split("\\.")[0].replaceAll("\\s*", "");

            if (columnIndex >= tuple.getValues().size() || columnIndex == -2) {
                ignoreFlag = true;
                return 0;
            }

            return Integer.parseInt(tuple.getValue(columnIndex).replaceAll("\\s",""));
        }
        throw new IllegalArgumentException("Unsupported expression: " + expression);
    }

}