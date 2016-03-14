package com.lts.queue.mysql;

import com.lts.core.cluster.Config;
import com.lts.queue.*;

/**
 * @author Robert HG (254963746@qq.com) on 3/12/16.
 */
public class MysqlJobQueueFactory implements JobQueueFactory {

    @Override
    public CronJobQueue getCronJobQueue(Config config) {
        return new MysqlCronJobQueue(config);
    }

    @Override
    public ExecutableJobQueue getExecutableJobQueue(Config config) {
        return new MysqlExecutableJobQueue(config);
    }

    @Override
    public ExecutingJobQueue getExecutingJobQueue(Config config) {
        return new MysqlExecutingJobQueue(config);
    }

    @Override
    public JobFeedbackQueue getJobFeedbackQueue(Config config) {
        return new MysqlJobFeedbackQueue(config);
    }

    @Override
    public NodeGroupStore getNodeGroupStore(Config config) {
        return new MysqlNodeGroupStore(config);
    }

    @Override
    public SuspendJobQueue getSuspendJobQueue(Config config) {
        return new MysqlSuspendJobQueue(config);
    }

    @Override
    public PreLoader getPreLoader(Config config) {
        return new MysqlPreLoader(config);
    }
}
