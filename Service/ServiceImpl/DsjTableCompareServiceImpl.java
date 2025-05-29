package com.founder.service.impl;

import cloud.lightweight.boot.system.CloudServiceImpl;
import com.founder.dsj.bean.entity.*;
import com.founder.enums.TaskStatusEnum;
import com.founder.object.CompareRequest;
import com.founder.object.CompareResult;
import com.founder.object.TableStructure;
import com.founder.service.DsjTableCompareService;
import com.founder.utils.DataBillUtil;
import com.founder.utils.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import java.util.*;
@Service
public class DsjTableCompareServiceImpl extends CloudServiceImpl implements DsjTableCompareService   {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    @Override
    public CompareResult compareTable(DsjDataCompareEntity table1, DsjDataCompareEntity table2) {

        JdbcTemplate connectForTable1= DataBillUtil.createJdbcTemplate(table1);
        JdbcTemplate connectForTable2= DataBillUtil.createJdbcTemplate(table2);
        TableStructure tableStructureForTable1 = DataBillUtil.getTableStructure(connectForTable1, table1.getTableName());
        TableStructure tableStructureForTable2 = DataBillUtil.getTableStructure(connectForTable2, table2.getTableName());
        List<Map<String, Object>> tableData1 = DataBillUtil.fetchTableData(connectForTable1, table1.getTableName(), tableStructureForTable1.getColumns());
        List<Map<String, Object>> tableData2 = DataBillUtil.fetchTableData(connectForTable1, table2.getTableName(), tableStructureForTable2.getColumns());
        Map<String, Map<String, Object>> table1Fingerprints = DataBillUtil.generateFingerprints(tableData1);
        Map<String, Map<String, Object>> table2Fingerprints = DataBillUtil.generateFingerprints(tableData2);
        List<Map<String, Object>> incrementForTable1 = new ArrayList<>();
        List<Map<String, Object>> incrementForTable2 = new ArrayList<>();
        for (String fingerprint : table1Fingerprints.keySet()) {
            if (!table2Fingerprints.containsKey(fingerprint)) {
                incrementForTable1.add(table1Fingerprints.get(fingerprint));
            }
        }
        for (String fingerprint : table2Fingerprints.keySet()) {
            if (!table1Fingerprints.containsKey(fingerprint)) {
                incrementForTable2.add(table2Fingerprints.get(fingerprint));
            }
        }
        return new CompareResult(incrementForTable1,incrementForTable2);
    }
    @Override
    public void value(DsjBillEntity dsjBillEntity) {
        String content = dsjBillEntity.getContent();
        CompareRequest compareRequest = JsonUtil.jsonToObject(content,CompareRequest.class);
        DsjDataCompareEntity source = compareRequest.getSource();
        DsjDataCompareEntity target = compareRequest.getTarget();
        JdbcTemplate connectForSource= DataBillUtil.createJdbcTemplate(source);
        JdbcTemplate connectForTarget= DataBillUtil.createJdbcTemplate(target);
        TableStructure tableStructureForSource = DataBillUtil.getTableStructure(connectForSource, source.getTableName());
        TableStructure tableStructureForTarget = DataBillUtil.getTableStructure(connectForTarget, target.getTableName());
        List<String> intersection = DataBillUtil.getIntersection(tableStructureForSource.getColumns(), tableStructureForTarget.getColumns());
        if(intersection.isEmpty()){
            dsjBillEntity.setStatus(TaskStatusEnum.FAILED.getCode());
            throw new RuntimeException("任务执行失败，请检查表字段是否一致");
        }
        tableStructureForSource.setColumns(intersection);
        tableStructureForTarget.setColumns(intersection);
        List<Map<String, Object>> sourceData = DataBillUtil.fetchTableData(connectForSource, source.getTableName(), tableStructureForSource.getColumns());
        List<Map<String, Object>> targetData = DataBillUtil.fetchTableData(connectForTarget, target.getTableName(), tableStructureForTarget.getColumns());
        Map<String, Map<String, Object>> sourceFingerPrints = DataBillUtil.generateFingerprints(sourceData);
        Map<String, Map<String, Object>> targetFingerPrints = DataBillUtil.generateFingerprints(targetData);
        for (String fingerprint : sourceFingerPrints.keySet()) {
            if (!targetFingerPrints.containsKey(fingerprint)) {
                String[] split = fingerprint.split("\\|");
                for(int i=0;i<split.length;i++){
                    if(split[i].startsWith("id")){
                        String id = split[i].split("=")[1];
                        DsjBillValueResultEntity dsjBillValueResultEntity = new DsjBillValueResultEntity();
                        dsjBillValueResultEntity.setBillId(dsjBillEntity.getId());
                        dsjBillValueResultEntity.setObjectId(id);
                        dsjBillValueResultEntity.setDataSource("0");
                        logger.info(dsjBillValueResultEntity.toString());
                        dsjBillValueResultEntity.insertSelf();
                    }
                }
            }
        }
        for (String fingerprint : targetFingerPrints.keySet()) {
            if (!sourceFingerPrints.containsKey(fingerprint)) {
                String[] split = fingerprint.split("\\|");
                for(int i=0;i<split.length;i++){
                    if(split[i].startsWith("id")){
                        String id = split[i].split("=")[1];
                        DsjBillValueResultEntity dsjBillValueResultEntity = new DsjBillValueResultEntity();
                        dsjBillValueResultEntity.setBillId(dsjBillEntity.getId());
                        dsjBillValueResultEntity.setObjectId(id);
                        dsjBillValueResultEntity.setDataSource("1");
                        logger.info(dsjBillValueResultEntity.toString());
                        dsjBillValueResultEntity.insertSelf();
                    }
                }
            }
        }
    }
    @Override
    public void count(DsjBillEntity dsjBillEntity) {
        String content = dsjBillEntity.getContent();
        CompareRequest compareRequest = JsonUtil.jsonToObject(content,CompareRequest.class);
        DsjDataCompareEntity source = compareRequest.getSource();
        DsjDataCompareEntity target = compareRequest.getTarget();
        JdbcTemplate connectForSource= DataBillUtil.createJdbcTemplate(source);
        JdbcTemplate connectForTarget= DataBillUtil.createJdbcTemplate(target);
        // 获取表名
        String sourceTableName = source.getTableName();
        String targetTableName = target.getTableName();
        Long sourceRowCount = connectForSource.queryForObject(DsjBillCountResultEntity.COUNT_SQL + sourceTableName, Long.class);
        Long targetRowCount = connectForTarget.queryForObject(DsjBillCountResultEntity.COUNT_SQL + targetTableName, Long.class);
        DsjBillCountResultEntity dsjBillCountResultEntity =new DsjBillCountResultEntity();
        dsjBillCountResultEntity.setBillId(dsjBillEntity.getId());
        dsjBillCountResultEntity.setSourceCount(sourceRowCount.toString());
        dsjBillCountResultEntity.setTargetCount(targetRowCount.toString());
        dsjBillCountResultEntity.insertSelf();
    }
    @Override
    public DsjBillEntity createTask(DsjBillEntity dsjBillEntity) {
        dsjBillEntity.setCreateTime(getTimeNow());
        dsjBillEntity.setStatus(TaskStatusEnum.PENDING.getCode());
        CompareRequest compareRequest = JsonUtil.jsonToObject(dsjBillEntity.getContent(), CompareRequest.class);
        dsjBillEntity.setSourceTableConnect(JsonUtil.objectToJson(compareRequest.getSource()));
        dsjBillEntity.setTargetTableConnect(JsonUtil.objectToJson(compareRequest.getTarget()));
        dsjBillEntity.insertSelf();
        return dsjBillEntity;
    }

}
