package ru.comptech2021.fliddhi.selector;

import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.input.stream.JoinInputStream;
import io.siddhi.query.api.expression.Variable;
import io.siddhi.query.api.expression.condition.Compare;
import org.apache.flink.types.Row;
import ru.comptech2021.fliddhi.FlinkRecord;

import java.util.Objects;

public class FliddhiJoinKeySelector extends FliddhiKeySelector {

    private final String leftAttributeName;
    private final String rightAttributeName;
    private final String leftInputStream;
    private final String rightInputStream;
    private final JoinInputStream.Type joinType;

    public FliddhiJoinKeySelector(SiddhiApp siddhiApp) {
        super(siddhiApp);

        Query query = (Query) siddhiApp.getExecutionElementList().get(0);
        JoinInputStream joinInputStream = (JoinInputStream) query.getInputStream();

        joinType = joinInputStream.getType();

        leftInputStream = joinInputStream.getLeftInputStream().getAllStreamIds().get(0);
        rightInputStream = joinInputStream.getRightInputStream().getAllStreamIds().get(0);

        Compare onCompare = (Compare) joinInputStream.getOnCompare();
        Variable leftExpression = (Variable) onCompare.getLeftExpression();
        Variable rightExpression = (Variable) onCompare.getRightExpression();
        leftAttributeName = leftExpression.getAttributeName();
        rightAttributeName = rightExpression.getAttributeName();

        System.out.println("FliddhiJoinKeySelector ctor");
    }

    @Override
    public String getKey(FlinkRecord record) throws Exception {
        Row row = record.getRow();

        switch (joinType) {
            case JOIN: // join = inner join
            case INNER_JOIN:
                System.out.println(leftInputStream + ":" + rightInputStream + ":" +
                        Objects.requireNonNull(row.getField(
                                siddhiApp.getStreamDefinitionMap()
                                        .get(leftInputStream)
                                        .getAttributePosition(leftAttributeName)
                        )).toString() + ":" +
                        Objects.requireNonNull(row.getField(
                                siddhiApp.getStreamDefinitionMap()
                                        .get(rightInputStream)
                                        .getAttributePosition(rightAttributeName)
                        )).toString());


                return leftInputStream + ":" + rightInputStream + ":" +
                        Objects.requireNonNull(row.getField(
                                siddhiApp.getStreamDefinitionMap()
                                        .get(leftInputStream)
                                        .getAttributePosition(leftAttributeName)
                        )).toString() +
                        Objects.requireNonNull(row.getField(
                                siddhiApp.getStreamDefinitionMap()
                                        .get(rightInputStream)
                                        .getAttributePosition(rightAttributeName)
                        )).toString();

            default: return row.getField(0).toString(); //заглушка
        }
    }
}
