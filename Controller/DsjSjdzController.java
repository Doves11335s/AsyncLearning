package com.founder.controller;


import com.founder.dsj.bean.entity.*;
import com.founder.enums.TaskStatusEnum;
import com.founder.object.CompareRequest;
import com.founder.service.AsyncCompareService;
import com.founder.service.DsjTableCompareService;
import com.founder.utils.JsonUtil;
import org.apache.poi.ss.formula.functions.T;
import org.setu.framework.core.component.SetuResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;


/**
 * 数据对账单
 */
@RestController
@RequestMapping("/compare")
public class DsjSjdzController {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    DsjTableCompareService dsjTableCompareService;
    @Autowired
    AsyncCompareService asyncCompareService;
    //创建对账单
    @PostMapping("/createTask")
    public SetuResult createTask(DsjBillEntity dsjBillEntity){
        String content = dsjBillEntity.getContent();
        dsjBillEntity = this.dsjTableCompareService.createTask(dsjBillEntity);
        return new SetuResult(dsjBillEntity);
    }
    //根据id查询对账单
    @GetMapping("/getBillById")
    public SetuResult getBillById(String id){
        DsjBillEntity dsjBillEntity = new DsjBillEntity();
        dsjBillEntity.setId(id);
        dsjBillEntity.queryBySelfId();
        return new SetuResult(dsjBillEntity);
    }
     //开始对账
     @PostMapping("/startTaskByBillId")
     public SetuResult startTask(String id){
         asyncCompareService.executeCompareTask(id);
         return new SetuResult();
     }
     //获取对账任务结果。
     @GetMapping("/getResultById")
     public SetuResult getTaskResult(@RequestParam String id){
         DsjBillEntity dsjBillEntity =new DsjBillEntity();
         dsjBillEntity.setId(id);
         dsjBillEntity.queryBySelfId();
         if(!dsjBillEntity.getStatus().equals(TaskStatusEnum.SUCCESS.getCode())){
             throw new RuntimeException("该任务没有执行成功");
         }
         List<?> res =new ArrayList<>();
         if(dsjBillEntity.getBillType().equals("1")){
             DsjBillValueResultEntity dsjBillValueResultEntity = new DsjBillValueResultEntity();
             dsjBillValueResultEntity.setBillId(id);
             res = dsjBillValueResultEntity.queryListBySelf();
         }else {
             DsjBillCountResultEntity dsjBillCountResultEntity = new DsjBillCountResultEntity();
             dsjBillCountResultEntity.setBillId(id);
             res=dsjBillCountResultEntity.queryListBySelf();
         }
         return new SetuResult(res);
     }
}
