package ru.comptech2021.fliddhi.selector;

import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.expression.Variable;
import org.apache.flink.types.Row;
import ru.comptech2021.fliddhi.FlinkRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FliddhiGroupByKeySelector extends FliddhiKeySelector {

    private final String nameOfInputStream;
    private final List<String> groupByAttributes = new ArrayList<>();

    public FliddhiGroupByKeySelector(SiddhiApp siddhiApp) {
        super(siddhiApp);
        System.out.println("FliddhiGroupByKeySelector ctor");

        Query query = (Query) siddhiApp.getExecutionElementList().get(0);

        List<Variable> groupByList = query.getSelector().getGroupByList();
        for (int i = 0; i < groupByList.size(); i++)
            groupByAttributes.add(i, groupByList.get(i).getAttributeName());

        //groupByAttribute = query.getSelector().getGroupByList().get(0).getAttributeName();
        nameOfInputStream = query.getInputStream().getAllStreamIds().get(0);
    }

    @Override
    public String getKey(FlinkRecord flinkRecord) throws Exception {
        Row row = flinkRecord.row;

        StringBuilder stringBuilder = new StringBuilder(nameOfInputStream);
        for (String groupByAttribute : groupByAttributes) {
            stringBuilder.append(":");
            stringBuilder.append(Objects.requireNonNull(row.getField(
                    siddhiApp.getStreamDefinitionMap().get(nameOfInputStream).
                            getAttributePosition(groupByAttribute))).toString());
        }

        System.out.println(stringBuilder.toString());
        return stringBuilder.toString();


/*
        System.out.println(nameOfInputStream + ":" +
                row.getField(
                        siddhiApp.getStreamDefinitionMap()
                                .get(nameOfInputStream)
                                .getAttributePosition(groupByAttribute)
                ).toString());

        return nameOfInputStream + ":" +
                row.getField(
                siddhiApp.getStreamDefinitionMap()
                        .get(nameOfInputStream)
                        .getAttributePosition(groupByAttribute)
                ).toString();

 */
    }
}
