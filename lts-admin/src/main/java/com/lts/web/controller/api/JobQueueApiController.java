package com.lts.web.controller.api;

import com.lts.biz.logger.domain.JobLogPo;
import com.lts.biz.logger.domain.JobLoggerRequest;
import com.lts.biz.logger.domain.LogType;
import com.lts.core.cluster.Node;
import com.lts.core.cluster.NodeType;
import com.lts.core.cmd.*;
import com.lts.core.commons.utils.Assert;
import com.lts.core.commons.utils.CollectionUtils;
import com.lts.core.commons.utils.StringUtils;
import com.lts.core.constant.Level;
import com.lts.core.domain.Job;
import com.lts.core.domain.KVPair;
import com.lts.core.json.JSON;
import com.lts.core.support.CronExpression;
import com.lts.core.support.CronExpressionUtils;
import com.lts.core.support.JobDomainConverter;
import com.lts.core.support.SystemClock;
import com.lts.queue.domain.JobPo;
import com.lts.queue.exception.DuplicateJobException;
import com.lts.web.cluster.AdminAppContext;
import com.lts.web.controller.AbstractController;
import com.lts.web.repository.memory.NodeMemoryDatabase;
import com.lts.web.request.JobQueueRequest;
import com.lts.web.request.NodeRequest;
import com.lts.web.response.PageResponse;
import com.lts.web.vo.RestfulResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author Robert HG (254963746@qq.com) on 6/6/15.
 */
@RestController
public class JobQueueApiController extends AbstractController {

    @Autowired
    AdminAppContext appContext;
    @Autowired
    NodeMemoryDatabase nodeMemoryDatabase;

    @RequestMapping("/job-queue/cron-job-get")
    public RestfulResponse cronJobGet(JobQueueRequest request) {
        PageResponse<JobPo> pageResponse = appContext.getCronJobQueue().pageSelect(request);
        RestfulResponse response = new RestfulResponse();
        response.setSuccess(true);
        response.setResults(pageResponse.getResults());
        response.setRows(pageResponse.getRows());
        return response;
    }

    @RequestMapping("/job-queue/executable-job-get")
    public RestfulResponse executableJobGet(JobQueueRequest request) {
        PageResponse<JobPo> pageResponse = appContext.getExecutableJobQueue().pageSelect(request);
        RestfulResponse response = new RestfulResponse();
        response.setSuccess(true);
        response.setResults(pageResponse.getResults());
        response.setRows(pageResponse.getRows());
        return response;
    }

    @RequestMapping("/job-queue/executing-job-get")
    public RestfulResponse executingJobGet(JobQueueRequest request) {
        PageResponse<JobPo> pageResponse = appContext.getExecutingJobQueue().pageSelect(request);
        RestfulResponse response = new RestfulResponse();
        response.setSuccess(true);
        response.setResults(pageResponse.getResults());
        response.setRows(pageResponse.getRows());
        return response;
    }

    @RequestMapping("/job-queue/suspend-job-get")
    public RestfulResponse suspendJobGet(JobQueueRequest request) {
        PageResponse<JobPo> pageResponse = appContext.getSuspendJobQueue().pageSelect(request);
        RestfulResponse response = new RestfulResponse();
        response.setSuccess(true);
        response.setResults(pageResponse.getResults());
        response.setRows(pageResponse.getRows());
        return response;
    }

    @RequestMapping("/job-queue/cron-job-update")
    public RestfulResponse cronJobUpdate(JobQueueRequest request) {
        RestfulResponse response = new RestfulResponse();
        // 检查参数
        try {
            Assert.hasLength(request.getJobId(), "jobId不能为空!");
            Assert.hasLength(request.getCronExpression(), "cronExpression不能为空!");
        } catch (IllegalArgumentException e) {
            response.setSuccess(false);
            response.setMsg(e.getMessage());
            return response;
        }
        // 1. 检测 cronExpression是否是正确的
        try {
            CronExpression expression = new CronExpression(request.getCronExpression());
            if (expression.getTimeAfter(new Date()) == null) {
                response.setSuccess(false);
                response.setMsg(StringUtils.format("该CronExpression={} 已经没有执行时间点! 请重新设置或者直接删除。", request.getCronExpression()));
                return response;
            }

            boolean success = appContext.getCronJobQueue().selectiveUpdate(request);
            if (success) {
                try {
                    // 把等待执行的队列也更新一下
                    request.setTriggerTime(expression.getTimeAfter(new Date()));
                    appContext.getExecutableJobQueue().selectiveUpdate(request);
                } catch (Exception e) {
                    response.setSuccess(false);
                    response.setMsg("更新等待执行的任务失败，请手动更新! error:" + e.getMessage());
                    return response;
                }
                response.setSuccess(true);
            } else {
                response.setSuccess(false);
                response.setMsg("该任务已经被删除或者执行完成.");
            }
            return response;
        } catch (ParseException e) {
            response.setSuccess(false);
            response.setMsg("请输入正确的 CronExpression!");
            return response;
        }
    }

    @RequestMapping("/job-queue/executable-job-update")
    public RestfulResponse executableJobUpdate(JobQueueRequest request) {
        RestfulResponse response = new RestfulResponse();
        // 检查参数
        // 1. 检测 cronExpression是否是正确的
        if (StringUtils.isNotEmpty(request.getCronExpression())) {
            try {
                CronExpression expression = new CronExpression(request.getCronExpression());
                if (expression.getTimeAfter(new Date()) == null) {
                    response.setSuccess(false);
                    response.setMsg(StringUtils.format("该CronExpression={} 已经没有执行时间点!", request.getCronExpression()));
                    return response;
                }
            } catch (ParseException e) {
                response.setSuccess(false);
                response.setMsg("请输入正确的 CronExpression!");
                return response;
            }
        }
        try {
            Assert.hasLength(request.getJobId(), "jobId不能为空!");
            Assert.hasLength(request.getTaskTrackerNodeGroup(), "taskTrackerNodeGroup不能为空!");
        } catch (IllegalArgumentException e) {
            response.setSuccess(false);
            response.setMsg(e.getMessage());
            return response;
        }
        boolean success = appContext.getExecutableJobQueue().selectiveUpdate(request);
        if (success) {
            response.setSuccess(true);
        } else {
            response.setSuccess(false);
            response.setCode("DELETE_OR_RUNNING");
        }
        return response;
    }

    @RequestMapping("/job-queue/suspend-job-update")
    public RestfulResponse suspendJobUpdate(JobQueueRequest request) {
        RestfulResponse response = new RestfulResponse();
        // 检查参数
        try {
            Assert.hasLength(request.getJobId(), "jobId不能为空!");
            Assert.hasLength(request.getCronExpression(), "cronExpression不能为空!");
        } catch (IllegalArgumentException e) {
            response.setSuccess(false);
            response.setMsg(e.getMessage());
            return response;
        }
        // 1. 检测 cronExpression是否是正确的
        try {
            CronExpression expression = new CronExpression(request.getCronExpression());
            if (expression.getTimeAfter(new Date()) == null) {
                response.setSuccess(false);
                response.setMsg(StringUtils.format("该CronExpression={} 已经没有执行时间点! 请重新设置或者直接删除。", request.getCronExpression()));
                return response;
            }

            boolean success = appContext.getSuspendJobQueue().selectiveUpdate(request);
            if (success) {
                response.setSuccess(true);
            } else {
                response.setSuccess(false);
                response.setMsg("该任务已经被删除或者执行完成.");
            }
            return response;
        } catch (ParseException e) {
            response.setSuccess(false);
            response.setMsg("请输入正确的 CronExpression!");
            return response;
        }
    }

    @RequestMapping("/job-queue/cron-job-delete")
    public RestfulResponse cronJobDelete(JobQueueRequest request) {
        RestfulResponse response = new RestfulResponse();
        if (StringUtils.isEmpty(request.getJobId())) {
            response.setSuccess(false);
            response.setMsg("JobId 必须传!");
            return response;
        }
        boolean success = appContext.getCronJobQueue().remove(request.getJobId());
        if (success) {
            try {
                appContext.getExecutableJobQueue().remove(request.getTaskTrackerNodeGroup(), request.getJobId());
            } catch (Exception e) {
                response.setSuccess(false);
                response.setMsg("删除等待执行的任务失败，请手动删除! error:{}" + e.getMessage());
                return response;
            }
        }
        response.setSuccess(true);
        return response;
    }

    @RequestMapping("/job-queue/executable-job-delete")
    public RestfulResponse executableJobDelete(JobQueueRequest request) {
        RestfulResponse response = new RestfulResponse();
        try {
            Assert.hasLength(request.getJobId(), "jobId不能为空!");
            Assert.hasLength(request.getTaskTrackerNodeGroup(), "taskTrackerNodeGroup不能为空!");
        } catch (IllegalArgumentException e) {
            response.setSuccess(false);
            response.setMsg(e.getMessage());
            return response;
        }

        boolean success = appContext.getExecutableJobQueue().remove(request.getTaskTrackerNodeGroup(), request.getJobId());
        if (success) {
            if (StringUtils.isNotEmpty(request.getCronExpression())) {
                // 是Cron任务, Cron任务队列的也要被删除
                try {
                    appContext.getCronJobQueue().remove(request.getJobId());
                } catch (Exception e) {
                    response.setSuccess(false);
                    response.setMsg("在Cron任务队列中删除该任务失败，请手动更新! error:" + e.getMessage());
                    return response;
                }
            }
            response.setSuccess(true);
        } else {
            response.setSuccess(false);
            response.setMsg("更新失败，该条任务可能已经删除.");
        }

        return response;
    }

    @RequestMapping("/job-queue/suspend-job-delete")
    public RestfulResponse suspendJobDelete(JobQueueRequest request) {
        RestfulResponse response = new RestfulResponse();
        if (StringUtils.isEmpty(request.getJobId())) {
            response.setSuccess(false);
            response.setMsg("JobId 必须传!");
            return response;
        }
        boolean success = appContext.getSuspendJobQueue().remove(request.getJobId());
        response.setSuccess(success);
        return response;
    }

    @RequestMapping("/job-logger/job-logger-get")
    public RestfulResponse jobLoggerGet(JobLoggerRequest request) {
        RestfulResponse response = new RestfulResponse();

//        try {
//            Assert.hasLength(request.getTaskId(), "taskId不能为空!");
//            Assert.hasLength(request.getTaskTrackerNodeGroup(), "taskTrackerNodeGroup不能为空!");
//        } catch (IllegalArgumentException e) {
//            response.setSuccess(false);
//            response.setMsg(e.getMessage());
//            return response;
//        }

        PageResponse<JobLogPo> pageResponse = appContext.getJobLogger().search(request);
        response.setResults(pageResponse.getResults());
        response.setRows(pageResponse.getRows());

        response.setSuccess(true);
        return response;
    }

    /**
     * 给JobTracker发消息 加载任务到内存
     */
    @RequestMapping("/job-queue/load-add")
    public RestfulResponse loadJob(JobQueueRequest request) {
        RestfulResponse response = new RestfulResponse();

        String nodeGroup = request.getTaskTrackerNodeGroup();

        HttpCmd httpCmd = new HttpCmd();
        httpCmd.setCommand(HttpCmds.CMD_LOAD_JOB);
        httpCmd.addParam("nodeGroup", nodeGroup);

        NodeRequest nodeRequest = new NodeRequest();
        nodeRequest.setNodeType(NodeType.JOB_TRACKER);
        List<Node> jobTrackerNodeList = nodeMemoryDatabase.search(nodeRequest);
        if (CollectionUtils.isEmpty(jobTrackerNodeList)) {
            response.setMsg("Can not found JobTracker.");
            response.setSuccess(false);
            return response;
        }

        boolean success = false;
        HttpCmdResponse cmdResponse = null;
        for (Node node : jobTrackerNodeList) {
            httpCmd.setNodeIdentity(node.getIdentity());
            cmdResponse = HttpCmdClient.execute(node.getIp(), node.getHttpCmdPort(), httpCmd);
            if (cmdResponse.isSuccess()) {
                success = true;
                break;
            }
        }
        if (success) {
            response.setMsg("Load success");
        } else {
            response.setMsg("Load failed");
        }
        response.setSuccess(success);
        return response;
    }

    @RequestMapping("/job-queue/job-add")
    public RestfulResponse jobAdd(JobQueueRequest request) {
        RestfulResponse response = new RestfulResponse();
        // 表单check

        try {
            Assert.hasLength(request.getTaskId(), "taskId不能为空!");
            Assert.hasLength(request.getTaskTrackerNodeGroup(), "taskTrackerNodeGroup不能为空!");
            if (request.getNeedFeedback()) {
                Assert.hasLength(request.getSubmitNodeGroup(), "submitNodeGroup不能为空!");
            }

            if (StringUtils.isNotEmpty(request.getCronExpression())) {
                try {
                    CronExpression expression = new CronExpression(request.getCronExpression());
                    Date nextTime = expression.getTimeAfter(new Date());
                    if (nextTime == null) {
                        response.setSuccess(false);
                        response.setMsg(StringUtils.format("该CronExpression={} 已经没有执行时间点!", request.getCronExpression()));
                        return response;
                    } else {
                        request.setTriggerTime(nextTime);
                    }
                } catch (ParseException e) {
                    response.setSuccess(false);
                    response.setMsg("请输入正确的 CronExpression!");
                    return response;
                }
            }

        } catch (IllegalArgumentException e) {
            response.setSuccess(false);
            response.setMsg(e.getMessage());
            return response;
        }

        KVPair<Boolean, String> pair = addJob(request);
        response.setSuccess(pair.getKey());
        response.setMsg(pair.getValue());
        return response;
    }

    @RequestMapping("/job-queue/cron-job-suspend")
    public RestfulResponse cronJobSuspend(JobQueueRequest request) {
        RestfulResponse response = new RestfulResponse();
        if (StringUtils.isEmpty(request.getJobId())) {
            response.setSuccess(false);
            response.setMsg("JobId 必须传!");
            return response;
        }
        JobPo jobPo = appContext.getCronJobQueue().finish(request.getJobId());
        if (jobPo == null) {
            response.setSuccess(false);
            response.setMsg("任务不存在，或者已经删除");
            return response;
        }
        try {
            appContext.getSuspendJobQueue().add(jobPo);
        } catch (DuplicateJobException e) {
            response.setSuccess(false);
            response.setMsg("改任务已经被暂停, 请检查暂停队列");
            return response;
        } catch (Exception e) {
            response.setSuccess(false);
            response.setMsg("移动任务到暂停队列失败, error:" + e.getMessage());
            return response;
        }
        try {
            appContext.getCronJobQueue().remove(request.getJobId());
        } catch (Exception e) {
            response.setSuccess(false);
            response.setMsg("删除Cron任务失败，请手动删除! error:" + e.getMessage());
            return response;
        }
        try {
            appContext.getExecutableJobQueue().remove(request.getTaskTrackerNodeGroup(), request.getJobId());
        } catch (Exception e) {
            response.setSuccess(false);
            response.setMsg("删除等待执行的任务失败，请手动删除! error:" + e.getMessage());
            return response;
        }

        // 记录日志
        JobLogPo jobLogPo = JobDomainConverter.convertJobLog(jobPo);
        jobLogPo.setSuccess(true);
        jobLogPo.setLogType(LogType.SUSPEND);
        jobLogPo.setLogTime(SystemClock.now());
        jobLogPo.setLevel(Level.INFO);
        appContext.getJobLogger().log(jobLogPo);

        response.setSuccess(true);
        return response;
    }

    @RequestMapping("/job-queue/suspend-job-recovery")
    public RestfulResponse suspendJobRecovery(JobQueueRequest request) {
        RestfulResponse response = new RestfulResponse();
        if (StringUtils.isEmpty(request.getJobId())) {
            response.setSuccess(false);
            response.setMsg("JobId 必须传!");
            return response;
        }

        JobPo jobPo = appContext.getSuspendJobQueue().finish(request.getJobId());
        if (jobPo == null) {
            response.setSuccess(false);
            response.setMsg("任务不存在，或者已经删除");
            return response;
        }

        // 先恢复,才能删除
        Date nextTriggerTime = CronExpressionUtils.getNextTriggerTime(jobPo.getCronExpression());
        if (nextTriggerTime != null) {
            jobPo.setGmtModified(SystemClock.now());
            try {
                // 1.add to cron job queue
                appContext.getCronJobQueue().add(jobPo);
            } catch (DuplicateJobException e) {
                response.setSuccess(false);
                response.setMsg("Cron队列中任务已经存在，请检查");
                return response;
            } catch (Exception e) {
                response.setSuccess(false);
                response.setMsg("插入Cron队列中任务错误, error:" + e.getMessage());
                return response;
            }

            try {
                // 2. add to executable queue
                jobPo.setTriggerTime(nextTriggerTime.getTime());
                appContext.getExecutableJobQueue().add(jobPo);
            } catch (DuplicateJobException e) {
                response.setSuccess(false);
                response.setMsg("等待执行队列中任务已经存在，请检查");
                return response;
            } catch (Exception e) {
                response.setSuccess(false);
                response.setMsg("插入等待执行队列中任务错误, error:" + e.getMessage());
                return response;
            }
        } else {
            response.setSuccess(false);
            response.setMsg("该任务已经无效, 或者已经没有下一轮执行时间点");
            return response;
        }

        // 从暂停表中移除
        if (!appContext.getSuspendJobQueue().remove(request.getJobId())) {
            response.setSuccess(false);
            response.setMsg("恢复暂停任务失败，请重试");
            return response;
        }

        // 记录日志
        JobLogPo jobLogPo = JobDomainConverter.convertJobLog(jobPo);
        jobLogPo.setSuccess(true);
        jobLogPo.setLogType(LogType.RESUME);
        jobLogPo.setLogTime(SystemClock.now());
        jobLogPo.setLevel(Level.INFO);
        appContext.getJobLogger().log(jobLogPo);

        response.setSuccess(true);
        return response;
    }

    private KVPair<Boolean, String> addJob(JobQueueRequest request) {

        Job job = new Job();
        job.setTaskId(request.getTaskId());
        if (CollectionUtils.isNotEmpty(request.getExtParams())) {
            for (Map.Entry<String, String> entry : request.getExtParams().entrySet()) {
                job.setParam(entry.getKey(), entry.getValue());
            }
        }
        // 执行节点的group名称
        job.setTaskTrackerNodeGroup(request.getTaskTrackerNodeGroup());
        job.setSubmitNodeGroup(request.getSubmitNodeGroup());

        job.setNeedFeedback(request.getNeedFeedback());
        job.setReplaceOnExist(true);
        // 这个是 cron expression 和 quartz 一样，可选
        job.setCronExpression(request.getCronExpression());
        if (request.getTriggerTime() != null) {
            job.setTriggerTime(request.getTriggerTime().getTime());
        }
        job.setPriority(request.getPriority());

        return addJob(job);
    }

    private KVPair<Boolean, String> addJob(Job job) {
        HttpCmd httpCmd = new DefaultHttpCmd();
        httpCmd.setCommand(HttpCmds.CMD_ADD_JOB);
        httpCmd.addParam("job", JSON.toJSONString(job));

        NodeRequest nodeRequest = new NodeRequest();
        nodeRequest.setNodeType(NodeType.JOB_TRACKER);
        List<Node> jobTrackerNodeList = nodeMemoryDatabase.search(nodeRequest);
        if (CollectionUtils.isEmpty(jobTrackerNodeList)) {
            return new KVPair<Boolean, String>(false, "Can not found JobTracker.");
        }

        HttpCmdResponse response = null;
        for (Node node : jobTrackerNodeList) {
            httpCmd.setNodeIdentity(node.getIdentity());
            response = HttpCmdClient.execute(node.getIp(), node.getHttpCmdPort(), httpCmd);
            if (response.isSuccess()) {
                return new KVPair<Boolean, String>(true, "Add success");
            }
        }
        if (response != null) {
            return new KVPair<Boolean, String>(false, response.getMsg());
        } else {
            return new KVPair<Boolean, String>(false, "Add failed");
        }
    }
}
