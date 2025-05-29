package com.founder.service;

import com.founder.dsj.bean.entity.*;
import com.founder.object.CompareRequest;
import com.founder.object.CompareResult;

import java.util.List;
import java.util.Map;

public interface DsjTableCompareService {
    CompareResult compareTable(DsjDataCompareEntity table1, DsjDataCompareEntity table2);
    DsjBillEntity createTask(DsjBillEntity dsjBillEntity);

    void value(DsjBillEntity dsjBillEntity);

    void count(DsjBillEntity dsjBillEntity);
}
