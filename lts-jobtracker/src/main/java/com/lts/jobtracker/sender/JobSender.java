package com.lts.jobtracker.sender;

import com.lts.biz.logger.domain.JobLogPo;
import com.lts.biz.logger.domain.LogType;
import com.lts.core.constant.Level;
import com.lts.core.json.JSON;
import com.lts.core.logger.Logger;
import com.lts.core.logger.LoggerFactory;
import com.lts.core.support.JobDomainConverter;
import com.lts.core.support.LoggerName;
import com.lts.core.support.SystemClock;
import com.lts.jobtracker.domain.JobTrackerAppContext;
import com.lts.queue.domain.JobPo;
import com.lts.store.jdbc.exception.DupEntryException;

/**
 * @author Robert HG (254963746@qq.com) on 11/11/15.
 */
public class JobSender {

    private final Logger LOGGER = LoggerFactory.getLogger(LoggerName.JobTracker);

    private JobTrackerAppContext appContext;

    public JobSender(JobTrackerAppContext appContext) {
        this.appContext = appContext;
    }

    public SendResult send(String taskTrackerNodeGroup, String taskTrackerIdentity, SendInvoker invoker) {

        // 从mongo 中取一个可运行的job
        final JobPo jobPo = appContext.getPreLoader().take(taskTrackerNodeGroup, taskTrackerIdentity);
		if (jobPo == null) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Job push failed: no job! nodeGroup=" + taskTrackerNodeGroup + ", identity=" + taskTrackerIdentity);
			}
			return new SendResult(false, JobPushResult.NO_JOB);
		}

        // IMPORTANT: 这里要先切换队列
        try {
            jobPo.setGmtModified(jobPo.getGmtCreated());
            appContext.getExecutingJobQueue().add(jobPo);
        } catch (DupEntryException e) {
            LOGGER.warn("ExecutingJobQueue already exist:" + JSON.toJSONString(jobPo));
            appContext.getExecutableJobQueue().resume(jobPo);
            return new SendResult(false, JobPushResult.FAILED);
        }

		boolean removeSuccess = appContext.getExecutableJobQueue().remove(jobPo.getTaskTrackerNodeGroup(),
				jobPo.getJobId());
		if (!removeSuccess) {
			return new SendResult(false, JobPushResult.FAILED);
		}

		SendResult sendResult = new SendResult(false, JobPushResult.SYSTEM_ERROR);
		try {
			sendResult = invoker.invoke(jobPo);
		} catch (Exception e) {
			LOGGER.error("JobSender@send error,job:" + JSON.toJSONString(jobPo),e);
		}

        if (sendResult.isSuccess()) {
            // 记录日志
            JobLogPo jobLogPo = JobDomainConverter.convertJobLog(jobPo);
            jobLogPo.setSuccess(true);
            jobLogPo.setLogType(LogType.SENT);
            jobLogPo.setLogTime(SystemClock.now());
            jobLogPo.setLevel(Level.INFO);
            appContext.getJobLogger().log(jobLogPo);
		} else if (sendResult.getReturnValue() == JobPushResult.SYSTEM_ERROR) {
			LOGGER.warn("JobSender sendResult SENT_ERROR,jobPo:{}", jobPo);
			// 记录日志
			JobLogPo jobLogPo = JobDomainConverter.convertJobLog(jobPo);
			jobLogPo.setSuccess(false);
			jobLogPo.setLogType(LogType.SENT);
			jobLogPo.setLogTime(SystemClock.now());
			jobLogPo.setLevel(Level.INFO);
			appContext.getJobLogger().log(jobLogPo);

			// 队列切回来
			boolean needResume = true;
			try {
				jobPo.setIsRunning(true);
				jobPo.setGmtModified(SystemClock.now());
				appContext.getExecutableJobQueue().add(jobPo);
			} catch (DupEntryException e) {
				LOGGER.warn("ExecutableJobQueue already exist:" + JSON.toJSONString(jobPo));
				needResume = false;
			}
			appContext.getExecutingJobQueue().remove(jobPo.getJobId());
			if (needResume) {
				appContext.getExecutableJobQueue().resume(jobPo);
			}
		}

        return sendResult;
    }

    public interface SendInvoker {
        SendResult invoke(JobPo jobPo);
    }

    public static class SendResult {
        private boolean success;
        private Object returnValue;

        public SendResult(boolean success, Object returnValue) {
            this.success = success;
            this.returnValue = returnValue;
        }

        public boolean isSuccess() {
            return success;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }

        public Object getReturnValue() {
            return returnValue;
        }

        public void setReturnValue(Object returnValue) {
            this.returnValue = returnValue;
        }
    }

}
