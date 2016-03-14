package com.lts.admin.controller.api;

import com.lts.admin.cluster.AdminAppContext;
import com.lts.admin.response.PaginationRsp;
import com.lts.biz.logger.domain.JobLogPo;
import com.lts.biz.logger.domain.JobLoggerRequest;
import com.lts.biz.logger.domain.LogType;
import com.lts.core.cluster.NodeType;
import com.lts.core.commons.utils.*;
import com.lts.core.logger.Logger;
import com.lts.core.logger.LoggerFactory;
import com.lts.admin.controller.AbstractController;
import com.lts.admin.access.domain.AbstractMonitorDataPo;
import com.lts.admin.request.MonitorDataAddRequest;
import com.lts.admin.request.MonitorDataRequest;
import com.lts.admin.service.MonitorDataService;
import com.lts.admin.vo.RestfulResponse;
import com.lts.queue.domain.JobPo;
import com.lts.queue.domain.NodeGroupPo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

/**
 * @author Robert HG (254963746@qq.com) on 8/21/15.
 */
@RestController
public class MonitorApiController extends AbstractController {

    private Logger LOGGER = LoggerFactory.getLogger(MonitorApiController.class);
	@Autowired
	AdminAppContext appContext;
    @Autowired
    private MonitorDataService monitorDataService;

    @RequestMapping("/monitor/monitor-data-add")
    public RestfulResponse monitorDataAdd(MonitorDataAddRequest request) {
        RestfulResponse response = new RestfulResponse();

        try {
            Assert.notNull(request.getNodeType(), "nodeType can not be null");
            Assert.hasText(request.getIdentity(), "identity can not be null");
            Assert.hasText(request.getNodeGroup(), "nodeGroup can not be null");
            Assert.hasText(request.getData(), "data can not be null");

            if (NodeType.TASK_TRACKER.equals(request.getNodeType())) {
                monitorDataService.addTaskTrackerMonitorData(request);
            } else if (NodeType.JOB_TRACKER.equals(request.getNodeType())) {
                monitorDataService.addJobTrackerMonitorData(request);
            } else if (NodeType.JOB_CLIENT.equals(request.getNodeType())) {
                //
            }

            response.setSuccess(true);
            return response;

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            response.setSuccess(false);
            response.setMsg(e.getMessage());
            return response;
        }
    }

    @RequestMapping("/monitor/jvm-info-data-add")
    public RestfulResponse jvmInfoAdd(MonitorDataAddRequest request) {
        RestfulResponse response = new RestfulResponse();

        try {
            Assert.notNull(request.getNodeType(), "nodeType can not be null");
            Assert.hasText(request.getIdentity(), "identity can not be null");
            Assert.hasText(request.getNodeGroup(), "nodeGroup can not be null");
            Assert.hasText(request.getData(), "data can not be null");

            monitorDataService.addJVMInfoData(request);

            response.setSuccess(true);
            return response;

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            response.setSuccess(false);
            response.setMsg(e.getMessage());
            return response;
        }
    }


    @RequestMapping("/monitor/monitor-data-get")
    public RestfulResponse monitorDataGet(MonitorDataRequest request) {
        RestfulResponse response = new RestfulResponse();
        if (request.getNodeType() == null) {
            response.setSuccess(false);
            response.setMsg("nodeType can not be null.");
            return response;
        }
        if (request.getStartTime() == null || request.getEndTime() == null) {
            response.setSuccess(false);
            response.setMsg("Search time range must be input.");
            return response;
        }
        if (StringUtils.isNotEmpty(request.getIdentity())) {
            request.setNodeGroup(null);
        }
        List<? extends AbstractMonitorDataPo> rows = monitorDataService.queryMonitorDataSum(request);
        response.setSuccess(true);
        response.setRows(rows);
        response.setResults(CollectionUtils.sizeOf(rows));
        return response;
    }

	@RequestMapping("/monitor/monitor-fail-job")
	public RestfulResponse monitorFailJob() {
		JobLoggerRequest request = new JobLoggerRequest();
		request.setLogType(LogType.FINISHED.toString());
		request.setSuccess(false);
		request.setStartLogTime(DateUtils.addMinute(DateUtils.now(), -2));
		request.setEndLogTime(DateUtils.now());
		request.setLimit(1000);
		PaginationRsp<JobLogPo> logPos = appContext.getJobLogger().search(request);
		RestfulResponse response = new RestfulResponse();
		if (CollectionUtils.isEmpty(logPos.getRows())) {
			response.setSuccess(true);
		} else {
			List<Map<String, String>> rows = new ArrayList<Map<String, String>>();
			for (JobLogPo log : logPos.getRows()) {
				Map<String, String> map = new LinkedHashMap<String, String>();
				map.put("taskId", log.getTaskId());
				map.put("msg", log.getMsg());
				map.put("time", DateUtils.format(new Date(log.getLogTime()), DateUtils.YMD_HMS));
				rows.add(map);
			}
			response.setSuccess(false);
			response.setResults(rows.size());
			response.setRows(rows);
		}
		return response;
	}

	@RequestMapping("/monitor/monitor-no-run-job")
	public RestfulResponse monitorNoRunJob() {
		RestfulResponse response = new RestfulResponse();
		List<NodeGroupPo> ngpos = appContext.getNodeGroupStore().getNodeGroup(NodeType.TASK_TRACKER);

		List<Map<String, String>> rows = new ArrayList<Map<String, String>>();
		for (NodeGroupPo groupPo : ngpos) {
			List<JobPo> jobPos = appContext.getExecutableJobQueue().getJob(groupPo.getName(),
					DateUtils.addMinute(DateUtils.now(), -10));
			if (CollectionUtils.isNotEmpty(jobPos)) {
				for (JobPo job : jobPos) {
					Map<String, String> map = new LinkedHashMap<String, String>();
					map.put("taskId", job.getTaskId());
					map.put("time", DateUtils.format(new Date(job.getTriggerTime()), DateUtils.YMD_HMS));
					rows.add(map);
				}
			}
		}
		if (CollectionUtils.isEmpty(rows)) {
			response.setSuccess(true);
		} else {
			response.setSuccess(false);
			response.setResults(rows.size());
			response.setRows(rows);
		}
		return response;
	}

}
