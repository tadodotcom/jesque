package net.greghaines.jesque;

import net.greghaines.jesque.worker.MapBasedJobFactory;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerEvent;
import net.greghaines.jesque.worker.WorkerImpl;
import net.greghaines.jesque.worker.WorkerListener;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static net.greghaines.jesque.TestUtils.createJedis;
import static net.greghaines.jesque.utils.JesqueUtils.createKey;
import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;
import static net.greghaines.jesque.utils.ResqueConstants.FAILED;
import static net.greghaines.jesque.utils.ResqueConstants.PROCESSED;
import static net.greghaines.jesque.utils.ResqueConstants.STAT;

public class PipelineIntegrationTest {
    private static final Config CONFIG = new ConfigBuilder().build();
    private static final String TEST_QUEUE = "foo";

    @Before
    public void resetRedis() {
        TestUtils.resetRedis(CONFIG);
    }

    @Test
    public void jobSuccess() throws Exception {
        //LOG.info("Running jobSuccess()...");
        assertSuccess(null);
    }

    private static void assertSuccess(final WorkerListener listener, final WorkerEvent... events) {
        final Job job = new Job("TestAction", new Object[] { 1, 2.3, true, "test", Arrays.asList("inner", 4.5) });

        doWork(Arrays.asList(job), map(entry("TestAction", TestAction.class)), listener, events);

        final Jedis jedis = createJedis(CONFIG);
        try {
            Assert.assertEquals("1", jedis.get(createKey(CONFIG.getNamespace(), STAT, PROCESSED)));
            Assert.assertNull(jedis.get(createKey(CONFIG.getNamespace(), STAT, FAILED)));
        } finally {
            jedis.quit();
        }
    }

    private static void doWork(final List<Job> jobs, final Map<String, ? extends Class<? extends Runnable>> jobTypes,
                               final WorkerListener listener, final WorkerEvent... events) {
        final Worker worker = new WorkerImpl(CONFIG, Arrays.asList(TEST_QUEUE), new MapBasedJobFactory(jobTypes));
        if (listener != null && events.length > 0) {
            worker.getWorkerEventEmitter().addListener(listener, events);
        }
        final Thread workerThread = new Thread(worker);
        workerThread.start();
        try {
            TestUtils.pipelineJobs(TEST_QUEUE, jobs, CONFIG);
        } finally {
            TestUtils.stopWorker(worker, workerThread);
        }
    }
}
