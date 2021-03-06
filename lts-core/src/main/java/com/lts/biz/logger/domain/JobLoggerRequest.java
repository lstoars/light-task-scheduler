package com.lts.biz.logger.domain;

import com.lts.admin.request.PaginationReq;

import java.util.Date;

/**
 * @author Robert HG (254963746@qq.com) on 6/11/15.
 */
public class JobLoggerRequest extends PaginationReq {

    private String taskId;

    private String taskTrackerNodeGroup;

    private Date startLogTime;

    private Date endLogTime;

	private String logType;

	private Boolean success;

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskTrackerNodeGroup() {
        return taskTrackerNodeGroup;
    }

    public void setTaskTrackerNodeGroup(String taskTrackerNodeGroup) {
        this.taskTrackerNodeGroup = taskTrackerNodeGroup;
    }

    public Date getStartLogTime() {
        return startLogTime;
    }

    public void setStartLogTime(Date startLogTime) {
        this.startLogTime = startLogTime;
    }

    public Date getEndLogTime() {
        return endLogTime;
    }

    public void setEndLogTime(Date endLogTime) {
        this.endLogTime = endLogTime;
    }

	public String getLogType() {
		return logType;
	}

	public void setLogType(String logType) {
		this.logType = logType;
	}

	public Boolean getSuccess() {
		return success;
	}

	public String getSuccessStr() {
		if(success == null) {
			return null;
		}
		if(success) {
			return "1";
		} else {
			return "0";
		}
	}

	public void setSuccess(Boolean success) {
		this.success = success;
	}
}
