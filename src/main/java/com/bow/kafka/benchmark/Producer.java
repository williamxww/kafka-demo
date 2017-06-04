package com.bow.kafka.benchmark;

import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.bow.kafka.demo.producer.ProducerDemo;
import com.bow.kafka.util.MqProducer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {
	private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);

	public static void main(String[] args) throws Exception {

		final String topic = "PRICE";
		final String group = "group-a";
		final int threadCount = 1;
		final int messageSize = 1000;
		final int numPerThread = 100_0000;

		LOGGER.info("topic {} threadCount {} messageSize {} ", topic, threadCount, messageSize);

		final Logger log = LoggerFactory.getLogger(Producer.class);

		final String msg = buildMessage(messageSize);

		final ExecutorService sendThreadPool = Executors.newFixedThreadPool(threadCount);

		final StatsBenchmarkProducer statsBenchmark = new StatsBenchmarkProducer();

		final Timer timer = new Timer("BenchmarkTimerThread", true);

		final LinkedList<Long[]> snapshotList = new LinkedList();

		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				snapshotList.addLast(statsBenchmark.createSnapshot());
				if (snapshotList.size() > 10) {
					snapshotList.removeFirst();
				}
			}
		}, 1000, 1000);

		timer.scheduleAtFixedRate(new TimerTask() {
			private void printStats() {
				if (snapshotList.size() >= 10) {
					Long[] begin = snapshotList.getFirst();
					Long[] end = snapshotList.getLast();

					final long sendTps = (long) (((end[3] - begin[3]) / (double) (end[0] - begin[0])) * 1000L);
					final double averageRT = (end[5] - begin[5]) / (double) (end[3] - begin[3]);

					System.out.printf(
							"Send TPS:%d,  Max Send Time:%d,  Average Send Time:%7.3f,  Send Num:%d,  Send Failed:%d%n",
							sendTps, end[6], averageRT, end[1], end[2]);
				}
			}

			@Override
			public void run() {
				try {
					this.printStats();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}, 5000, 5000);

		final MqProducer producer = new MqProducer();

		for (int i = 0; i < threadCount; i++) {
			sendThreadPool.execute(new Runnable() {
				@Override
				public void run() {
					for (int i=0; i< numPerThread; i++) {
						try {
							final long beginTimestamp = System.currentTimeMillis();
							producer.send(topic, "key", msg, new Callback() {
								@Override
								public void onCompletion(RecordMetadata metadata, Exception exception) {
									statsBenchmark.getSendRequestSuccessCount().incrementAndGet();
									statsBenchmark.getReceiveResponseSuccessCount().incrementAndGet();
									final long currentRT = System.currentTimeMillis() - beginTimestamp;
									statsBenchmark.getSendMessageSuccessTimeTotal().addAndGet(currentRT);
									compareAndSetMax(statsBenchmark.getSendMessageMaxRT(), currentRT);
								}
							});

						} catch (Exception e) {
							statsBenchmark.getSendRequestFailedCount().incrementAndGet();
							// statsBenchmark.getReceiveResponseFailedCount().incrementAndGet();
							log.error("[BENCHMARK_PRODUCER] Send Exception", e);

							try {
								Thread.sleep(3000);
							} catch (InterruptedException ignored) {
							}
						}
					}
				}
			});
		}
	}

	public static void compareAndSetMax(final AtomicLong target, final long value) {
		long prev = target.get();
		while (value > prev) {
			boolean updated = target.compareAndSet(prev, value);
			if (updated) {
				break;
			}
			prev = target.get();
		}
	}

	private static String buildMessage(final int messageSize) throws UnsupportedEncodingException {

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < messageSize; i += 10) {
			sb.append("hello baby");
		}

		return sb.toString();
	}
}

class StatsBenchmarkProducer {
	private final AtomicLong sendRequestSuccessCount = new AtomicLong(0L);

	private final AtomicLong sendRequestFailedCount = new AtomicLong(0L);

	private final AtomicLong receiveResponseSuccessCount = new AtomicLong(0L);

	private final AtomicLong receiveResponseFailedCount = new AtomicLong(0L);

	private final AtomicLong sendMessageSuccessTimeTotal = new AtomicLong(0L);

	private final AtomicLong sendMessageMaxRT = new AtomicLong(0L);

	public Long[] createSnapshot() {
		Long[] snap = new Long[] { System.currentTimeMillis(), this.sendRequestSuccessCount.get(),
				this.sendRequestFailedCount.get(), this.receiveResponseSuccessCount.get(),
				this.receiveResponseFailedCount.get(), this.sendMessageSuccessTimeTotal.get(), sendMessageMaxRT.get() };
		// 重新统计下个阶段的最大时间
		this.sendMessageMaxRT.set(0);
		return snap;
	}

	public AtomicLong getSendRequestSuccessCount() {
		return sendRequestSuccessCount;
	}

	public AtomicLong getSendRequestFailedCount() {
		return sendRequestFailedCount;
	}

	public AtomicLong getReceiveResponseSuccessCount() {
		return receiveResponseSuccessCount;
	}

	public AtomicLong getReceiveResponseFailedCount() {
		return receiveResponseFailedCount;
	}

	public AtomicLong getSendMessageSuccessTimeTotal() {
		return sendMessageSuccessTimeTotal;
	}

	public AtomicLong getSendMessageMaxRT() {
		return sendMessageMaxRT;
	}
}
