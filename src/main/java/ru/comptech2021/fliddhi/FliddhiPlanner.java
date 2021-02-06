package ru.comptech2021.fliddhi;

import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.api.execution.ExecutionElement;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.input.stream.JoinInputStream;
import ru.comptech2021.fliddhi.selector.FliddhiGroupByKeySelector;
import ru.comptech2021.fliddhi.selector.FliddhiJoinKeySelector;
import ru.comptech2021.fliddhi.selector.FliddhiKeySelector;

import java.util.List;

public abstract class FliddhiPlanner {

     public static FliddhiKeySelector createFliddhiKeySelector(SiddhiApp siddhiApp) {
         List<ExecutionElement> executionElementList = siddhiApp.getExecutionElementList();

         for(int i = 0; i < executionElementList.size(); i++) {
             Query query = (Query) executionElementList.get(i);

             System.out.println("FliddhiKeySelectorPlanner");

             if (!query.getSelector().getGroupByList().isEmpty()) {
                 return new FliddhiGroupByKeySelector(siddhiApp, query);
             }
             if (query.getInputStream() instanceof JoinInputStream) {
                 return new FliddhiJoinKeySelector(siddhiApp);
             }
         }
         throw new UnsupportedOperationException();
    }
}
