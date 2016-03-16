package com.lts.core.monitor;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.lts.core.AppContext;
import com.lts.core.cluster.Config;
import com.lts.core.cluster.NodeType;
import com.lts.core.domain.monitor.MData;
import com.lts.core.factory.NamedThreadFactory;
import com.lts.core.logger.Logger;
import com.lts.core.logger.LoggerFactory;
import com.lts.jvmmonitor.JVMMonitor;

/**
 * @author Robert HG (254963746@qq.com) on 8/30/15.
 */
public abstract class AbstractMStatReporter implements MStatReporter {

    protected final Logger LOGGER = LoggerFactory.getLogger(AbstractMStatReporter.class);

    protected AppContext appContext;
    protected Config config;

    private ScheduledExecutorService executor = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory("LTS-Monitor-data-collector", true));
    private ScheduledFuture<?> scheduledFuture;
    private AtomicBoolean start = new AtomicBoolean(false);

    public AbstractMStatReporter(AppContext appContext) {
        this.appContext = appContext;
        this.config = appContext.getConfig();
    }

    public final void start() {

        // 启动JVM监控
        JVMMonitor.start();

		boolean enabled = config.getParameter("stat.report.enabled", false);
		if (!enabled) {
			return;
		}

        try {
            if (start.compareAndSet(false, true)) {
                scheduledFuture = executor.scheduleWithFixedDelay(
                        new MStatReportWorker(appContext, this), 1, 1, TimeUnit.SECONDS);
                LOGGER.info("MStatReporter start succeed.");
            }
        } catch (Exception e) {
            LOGGER.error("MStatReporter start failed.", e);
        }
    }

    /**
     * 用来收集数据
     */
    protected abstract MData collectMData();

    protected abstract NodeType getNodeType();

    public final void stop() {
        try {
            if (start.compareAndSet(true, false)) {
                scheduledFuture.cancel(true);
                executor.shutdown();
                JVMMonitor.stop();
                LOGGER.info("MStatReporter stop succeed.");
            }
        } catch (Exception e) {
            LOGGER.error("MStatReporter stop failed.", e);
        }
    }

}
