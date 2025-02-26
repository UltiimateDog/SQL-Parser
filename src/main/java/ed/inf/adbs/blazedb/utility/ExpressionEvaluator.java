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
 * ExpressionEvaluator evaluates WHERE clause conditions on a given Tuple.
 * It supports different types of binary expressions and arithmetic evaluations like SUM.
 * The class handles logical conditions like AND, comparison operators (>, <, =, !=), and arithmetic operations.
 */
public class ExpressionEvaluator {

    private final List<String> tableOrder;
    private final Tuple tuple;
    private Boolean ignoreFlag;
    private String currentTables;
    @Getter
    private List<String> tablesForExpression = new ArrayList<>();

    /**
     * Constructor for the ExpressionEvaluator class.
     * @param tableOrder The list of table names in the query.
     * @param tuple The tuple being evaluated for the WHERE clause.
     */
    public ExpressionEvaluator(List<String> tableOrder, Tuple tuple) {
        this.tableOrder = tableOrder;
        this.tuple = tuple;
    }

    /**
     * Evaluates an arithmetic SUM expression on the tuple.
     * This method assumes that the expression is a multiplication (e.g., SUM(col1 * col2)).
     * @param expression The SUM expression as a string.
     * @return The computed product value for the given expression.
     */
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
     * Evaluates a WHERE clause expression on the tuple.
     * This method handles logical AND expressions and various comparison operators (>, <, =, !=).
     * @param expression The WHERE clause expression to evaluate.
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
        return false;
    }

    /**
     * Evaluates a binary comparison expression like column1 = column2 or column1 > column2.
     * @param expression The binary expression to evaluate (e.g., column1 = column2).
     * @param operator The comparison operator (e.g., '=', '>', '<').
     * @return True if the comparison holds, otherwise false.
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
     * Extracts the integer value from a given expression (could be a constant or column reference).
     * @param expression The expression to extract the value from (could be a column or literal).
     * @return The integer value of the expression.
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