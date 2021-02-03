package ru.comptech2021.fliddhi;

import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.input.stream.JoinInputStream;
import io.siddhi.query.api.expression.Variable;
import io.siddhi.query.api.expression.condition.Compare;
import org.apache.flink.types.Row;

import java.util.Objects;

//не очень уверен в этой реализации))))
public class FliddhiJoinKeySelector extends FliddhiKeySelector {

    private final String onAttributeName;
    private final String inputStream;

    public FliddhiJoinKeySelector(SiddhiApp siddhiApp) {
        super(siddhiApp);

        Query query = (Query) siddhiApp.getExecutionElementList().get(0);
        JoinInputStream joinInputStream = (JoinInputStream) query.getInputStream();

        JoinInputStream.Type joinType = joinInputStream.getType();

        inputStream = joinInputStream.getLeftInputStream().getAllStreamIds().get(0);

        Compare onCompare = (Compare) joinInputStream.getOnCompare();
        Variable leftExpression = (Variable) onCompare.getLeftExpression();
        onAttributeName = leftExpression.getAttributeName();

        System.out.println("FliddhiJoinKeySelector ctor");
    }

    @Override
    public String getKey(Row row) throws Exception {
        System.out.println(inputStream + ":" +
                Objects.requireNonNull(row.getField(
                        siddhiApp.getStreamDefinitionMap()
                                .get(inputStream)
                                .getAttributePosition(onAttributeName)
                )).toString());

        return inputStream + ":" +
                Objects.requireNonNull(row.getField(
                        siddhiApp.getStreamDefinitionMap()
                                .get(inputStream)
                                .getAttributePosition(onAttributeName)
                )).toString();
    }
}
