
package com.bow.kafka.benchmark;

import java.util.LinkedList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import com.bow.kafka.demo.consumer.ConsumerDemo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer {

	private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

	public static void main(String[] args) throws Exception {

		String topic = "PRICE";
		String group = "group-a";

		LOGGER.info("topic {}, group {} ", topic, group);

		final StatsBenchmarkConsumer statsBenchmarkConsumer = new StatsBenchmarkConsumer();

		final Timer timer = new Timer("BenchmarkTimerThread", true);

		final LinkedList<Long[]> snapshotList = new LinkedList();

		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				snapshotList.addLast(statsBenchmarkConsumer.createSnapshot());
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
					// 0时间戳，1总接收数，2总born-consume消耗时间，3总store2Consume时间
					// 4born-consume最大消耗时间，5 store2Consume最大时间
					final long consumeTps = (long) (((end[1] - begin[1]) / (double) (end[0] - begin[0])) * 1000L);
					final double averageB2CRT = (end[2] - begin[2]) / (double) (end[1] - begin[1]);
					// final double averageS2CRT = (end[3] - begin[3]) /
					// (double) (end[1] - begin[1]);

					System.out.printf("Consume TPS:%d,  Average Time(B2C):%7.3f,  MAX Time(B2C):%d,  Consume Total:%d%n", consumeTps,
							averageB2CRT, end[4], end[1]);
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

		ConsumerDemo consumer = new ConsumerDemo(group);
		consumer.subscribe(topic);
		System.out.printf("Consumer Started.%n");

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				long now = System.currentTimeMillis();

				statsBenchmarkConsumer.getReceiveMessageTotalCount().incrementAndGet();
				long born2ConsumerRT = now - record.timestamp();
				statsBenchmarkConsumer.getBorn2ConsumerTotalRT().addAndGet(born2ConsumerRT);

				long store2ConsumerRT = 0;
				// statsBenchmarkConsumer.getStore2ConsumerTotalRT().addAndGet(store2ConsumerRT);

				compareAndSetMax(statsBenchmarkConsumer.getBorn2ConsumerMaxRT(), born2ConsumerRT);

				compareAndSetMax(statsBenchmarkConsumer.getStore2ConsumerMaxRT(), store2ConsumerRT);
			}
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
}

class StatsBenchmarkConsumer {
	/**
	 * 总接收数
	 */
	private final AtomicLong receiveMessageTotalCount = new AtomicLong(0L);

	/**
	 * 消息传递总时间
	 */
	private final AtomicLong born2ConsumerTotalRT = new AtomicLong(0L);

	/**
	 * 从MQ到消费者时间-暂不测
	 */
	private final AtomicLong store2ConsumerTotalRT = new AtomicLong(0L);

	private final AtomicLong born2ConsumerMaxRT = new AtomicLong(0L);

	private final AtomicLong store2ConsumerMaxRT = new AtomicLong(0L);

	public Long[] createSnapshot() {

		Long[] snap = new Long[] { System.currentTimeMillis(), this.receiveMessageTotalCount.get(),
				this.born2ConsumerTotalRT.get(), this.store2ConsumerTotalRT.get(), this.born2ConsumerMaxRT.get(),
				this.store2ConsumerMaxRT.get() };

		// 最大只是代表这段时间内的最大,在下一段时间内重新比较最大
		this.born2ConsumerMaxRT.set(0);
		this.store2ConsumerMaxRT.set(0);
		return snap;
	}

	public AtomicLong getReceiveMessageTotalCount() {
		return receiveMessageTotalCount;
	}

	public AtomicLong getBorn2ConsumerTotalRT() {
		return born2ConsumerTotalRT;
	}

	public AtomicLong getStore2ConsumerTotalRT() {
		return store2ConsumerTotalRT;
	}

	public AtomicLong getBorn2ConsumerMaxRT() {
		return born2ConsumerMaxRT;
	}

	public AtomicLong getStore2ConsumerMaxRT() {
		return store2ConsumerMaxRT;
	}
}
