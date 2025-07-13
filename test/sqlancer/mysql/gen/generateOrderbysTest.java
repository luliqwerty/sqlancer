package sqlancer.mysql.gen;

import org.junit.jupiter.api.Test;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.MySQLVisitor;
import sqlancer.mysql.ast.MySQLExpression;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class generateOrderbysTest {
    @Test
    public void testGenerateOrderbys() {
        // Mock MySQLGlobalState
        MySQLGlobalState state = mock(MySQLGlobalState.class);
        MainOptions options = new MainOptions();
        when(state.getOptions()).thenReturn(options);
        Randomly.initialize(options);

        // Mock Table and Columns
        MySQLColumn column = mock(MySQLColumn.class);
        MySQLTable table = mock(MySQLTable.class);
        when(table.getColumns()).thenReturn(Collections.singletonList(column));

        // Mock Schema
        MySQLSchema schema = mock(MySQLSchema.class);
        when(schema.getRandomTable()).thenReturn(table);
        when(state.getSchema()).thenReturn(schema);

        // test generate ORDER BY expressions
        MySQLExpressionGenerator orderByGenerator = new MySQLExpressionGenerator(state);
        orderByGenerator.setColumns(state.getSchema().getRandomTable().getColumns());
        List<MySQLExpression> expressions = orderByGenerator.generateOrderBys();

        assertNotNull(expressions);
        assertFalse(expressions.isEmpty());
        System.out.println(expressions.size());
        for (MySQLExpression expression : expressions) {
            assertNotNull(expression);
            System.out.println(MySQLVisitor.asString(expression));
        }
    }
}