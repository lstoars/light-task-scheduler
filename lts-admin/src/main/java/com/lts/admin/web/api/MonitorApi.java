package com.lts.admin.web.api;

import com.lts.admin.cluster.BackendAppContext;
import com.lts.admin.request.JobQueueReq;
import com.lts.admin.request.MDataPaginationReq;
import com.lts.admin.response.PaginationRsp;
import com.lts.admin.web.AbstractMVC;
import com.lts.admin.web.vo.RestfulResponse;
import com.lts.biz.logger.domain.JobLogPo;
import com.lts.biz.logger.domain.JobLoggerRequest;
import com.lts.biz.logger.domain.LogType;
import com.lts.core.cluster.NodeType;
import com.lts.core.commons.utils.CollectionUtils;
import com.lts.core.commons.utils.DateUtils;
import com.lts.core.commons.utils.StringUtils;
import com.lts.monitor.access.domain.MDataPo;
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
public class MonitorApi extends AbstractMVC {

	@Autowired
	private BackendAppContext appContext;

	@RequestMapping("/monitor/monitor-data-get")
	public RestfulResponse monitorDataGet(MDataPaginationReq request) {
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

		List<? extends MDataPo> rows = null;
		switch (request.getNodeType()) {
		case JOB_CLIENT:
			rows = appContext.getBackendJobClientMAccess().querySum(request);
			break;
		case JOB_TRACKER:
			rows = appContext.getBackendJobTrackerMAccess().querySum(request);
			break;
		case TASK_TRACKER:
			rows = appContext.getBackendTaskTrackerMAccess().querySum(request);
			break;
		}
		response.setSuccess(true);
		response.setRows(rows);
		response.setResults(CollectionUtils.sizeOf(rows));
		return response;
	}

	@RequestMapping("/monitor/jvm-monitor-data-get")
	public RestfulResponse jvmMDataGet(MDataPaginationReq request) {
		RestfulResponse response = new RestfulResponse();
		if (request.getJvmType() == null) {
			response.setSuccess(false);
			response.setMsg("jvmType can not be null.");
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

		List<? extends MDataPo> rows = null;
		switch (request.getJvmType()) {
		case GC:
			rows = appContext.getBackendJVMGCAccess().queryAvg(request);
			break;
		case MEMORY:
			rows = appContext.getBackendJVMMemoryAccess().queryAvg(request);
			break;
		case THREAD:
			rows = appContext.getBackendJVMThreadAccess().queryAvg(request);
			break;
		}
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
					DateUtils.addMinute(DateUtils.now(), -60));
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

	@RequestMapping("/monitor/monitor-run-too-long-job")
	public RestfulResponse monitorRunTooLongJob() {
		RestfulResponse response = new RestfulResponse();
		JobQueueReq req = new JobQueueReq();
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.MINUTE,-5);
		req.setTriggerTime(calendar.getTime());
		req.setLimit(20);
		PaginationRsp<JobPo> jobs = appContext.getExecutingJobQueue().pageSelect(req);
		if(CollectionUtils.isEmpty(jobs.getRows())) {
			response.setSuccess(true);
			return response;
		}

		List<Map<String, String>> rows = new ArrayList<Map<String, String>>();
		for (JobPo job : jobs.getRows()) {
			Map<String, String> map = new LinkedHashMap<String, String>();
			map.put("taskId", job.getTaskId());
			map.put("time", DateUtils.format(new Date(job.getTriggerTime()), DateUtils.YMD_HMS));
			rows.add(map);
		}

		if (CollectionUtils.isEmpty(rows)) {
			response.setSuccess(true);
		} else {
			response.setSuccess(false);
			response.setResults(jobs.getRows().size());
			response.setRows(rows);
		}
		return response;
	}

}
