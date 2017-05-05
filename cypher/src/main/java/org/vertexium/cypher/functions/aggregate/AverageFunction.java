package org.vertexium.cypher.functions.aggregate;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.executor.ExpressionScope;
import org.vertexium.cypher.utils.ObjectUtils;

import java.util.List;

public class AverageFunction extends AggregationFunction {
    @Override
    public Object invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope) {
        assertArgumentCount(arguments, 1);
        Object arg0 = ctx.getExpressionExecutor().executeExpression(ctx, arguments[0], scope);

        if (arg0 instanceof List) {
            List<?> list = (List<?>) arg0;
            if (list.size() == 0) {
                return null;
            }
            return ObjectUtils.sumNumbers(list).doubleValue() / (double) list.size();
        }

        throw new VertexiumCypherTypeErrorException(arg0, List.class);
    }
}
