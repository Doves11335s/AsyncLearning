package com.founder.service.impl;

import cloud.lightweight.boot.system.CloudServiceImpl;
import com.founder.dsj.bean.entity.*;
import com.founder.enums.TaskStatusEnum;
import com.founder.object.CompareRequest;
import com.founder.object.CompareResult;
import com.founder.service.AsyncCompareService;
import com.founder.service.DsjTableCompareService;
import com.founder.utils.JsonUtil;
import javafx.concurrent.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;
@Service
public class AsyncCompareServiceImpl extends CloudServiceImpl implements AsyncCompareService {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    DsjTableCompareService dsjTableCompareService;
    @Override
    @Async("taskExecutor") // 指定异步线程池
    @Transactional
    public void executeCompareTask(String id){
        logger.info("开始对账");
        //根据id查任务
        DsjBillEntity dsjBillEntity = new DsjBillEntity();
        dsjBillEntity.setId(id);
        dsjBillEntity.queryBySelfId();
        //如果任务不存在 报错
        if(dsjBillEntity.getCreateTime()==null){
            throw new RuntimeException("该任务不存在。");
        }
        dsjBillEntity.setStatus(TaskStatusEnum.RUNNING.getCode());
        dsjBillEntity.setStartTime(getTimeNow());
        dsjBillEntity.updateBySelfId();
        logger.info("对账类型是："+dsjBillEntity.getBillType());
        if(dsjBillEntity.getBillType().equals("1")){
            logger.info("开始对账了嗷");
            this.dsjTableCompareService.value(dsjBillEntity);
        }
        else{
           this.dsjTableCompareService.count(dsjBillEntity);
        }

        dsjBillEntity.setEndTime(getTimeNow());
        dsjBillEntity.setStatus(TaskStatusEnum.SUCCESS.getCode());
        dsjBillEntity.updateBySelfId();
//        //如果执行失败
//        if(result.getIncrementForTable1().get(0).containsKey("执行失败")){
//            //更新结束时间
//            dsjBillEntity.setEndTime(getTimeNow());
//            //更新任务状态
//            dsjBillEntity.setStatus(TaskStatusEnum.FAILED.getCode());
//
//            //更新表中状态
//            dsjBillEntity.updateBySelfId();
//        }
//        //成功
//
//        dsjBillEntity.setEndTime(getTimeNow());
//        dsjBillEntity.setStatus(TaskStatusEnum.SUCCESS.getCode());


        //todo
        //保存结果



//        CompareResultEntity compareResultEntity = new CompareResultEntity();
//        //设置结果数据对应的任务id
//        compareResultEntity.setTaskId(taskId);
//        //设置结果的创建时间
//        compareResultEntity.setCreateTime(getTimeNow());
//        //将结果转为json存入表
//        compareResultEntity.setCompareResult(JsonUtil.objectToJson(result));
//
//        compareResultEntity.insertSelf();
    }



}
