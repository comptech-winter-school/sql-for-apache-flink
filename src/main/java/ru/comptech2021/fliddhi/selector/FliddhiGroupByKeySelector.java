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

    public FliddhiGroupByKeySelector(SiddhiApp siddhiApp, Query query) {
        super(siddhiApp);
        System.out.println("FliddhiGroupByKeySelector ctor");

        List<Variable> groupByList = query.getSelector().getGroupByList();
        for (int i = 0; i < groupByList.size(); i++)
            groupByAttributes.add(i, groupByList.get(i).getAttributeName());

        nameOfInputStream = query.getInputStream().getAllStreamIds().get(0);
    }

    @Override
    public String getKey(FlinkRecord flinkRecord) throws Exception {
        Row row = flinkRecord.getRow();

        StringBuilder stringBuilder = new StringBuilder(nameOfInputStream);
        for (String groupByAttribute : groupByAttributes) {
            stringBuilder.append(":");
            stringBuilder.append(Objects.requireNonNull(row.getField(
                    siddhiApp.getStreamDefinitionMap().get(nameOfInputStream).
                            getAttributePosition(groupByAttribute))).toString());
        }

        //System.out.println(stringBuilder.toString());
        return stringBuilder.toString();
    }
}
