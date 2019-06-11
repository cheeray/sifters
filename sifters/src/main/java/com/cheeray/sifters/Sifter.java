package com.cheeray.sifters;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @param K Type of key of the target graded object.
 * @param D Type of the target graded object.
 * @author Chengwei.Yan
 */
public class Sifter<K, D extends Gradable<K>> {
	private final static Logger LOG = LoggerFactory.getLogger(Sifter.class);

	/**
	 * Create a builder for the sifter.
	 * @param initialBuckets The initial number of buckets.
	 * @param loadFactor Load factor of buckets map.
	 * @param concurrencyLevel Concurrency level of buckets.
	 */
	public static SiftBuilder sift(int initialBuckets, float loadFactor,
			int concurrencyLevel) {
		return new SiftBuilder(initialBuckets, loadFactor, concurrencyLevel);
	}

	/** Internal fork join pool. */
	private static ForkJoinPool FJP = new ForkJoinPool(
			Runtime.getRuntime().availableProcessors());
	/** Map of buckets, keyed by UUID. */
	private final ConcurrentHashMap<UUID, Bucket<K, D>> buckets;
	/** Scheduler. */
	private final ScheduledThreadPoolExecutor ex;
	/** Enable auto grading? */
	private final GradingConfig autoGrading;
	/** Is sinking in progress? */
	private final AtomicBoolean sinking = new AtomicBoolean(false);
	/** Any running schedule. */
	private ScheduledFuture<?> schedule;
	private Callable<Boolean> down;

	/**
	 * Constructor
	 */
	Sifter(int initialCapacity, float loadFactor, int concurrencyLevel,
			GradingConfig autoGrading) {
		this.buckets = new ConcurrentHashMap<>(initialCapacity, loadFactor,
				concurrencyLevel);
		this.ex = new ScheduledThreadPoolExecutor(
				Runtime.getRuntime().availableProcessors());
		this.ex.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
		this.schedule = null;
		this.autoGrading = autoGrading;
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				shutdown();
			}
		});
	}

	/**
	 * Grading on given grades.
	 * @param key Bucket key.
	 * @param cfg The grading configuration.
	 * @return a bucket to hold the sift results.
	 */
	Bucket<K, D> grading(UUID key, GradingConfig cfg) {
		return grading(key, cfg.getDelay(), cfg.getUnit(), cfg.getGrades());
	}

	/**
	 * Grading on given grades.
	 * @param key Bucket key.
	 * @param delay The delay before collect.
	 * @param unit The time unit of delay.
	 * @param grades Grades allowed to be collected.
	 * @return a bucket to hold the sift results.
	 */
	Bucket<K, D> grading(UUID key, long delay, TimeUnit unit, Grade<?>... grades) {
		LOG.info("{} is grading on {}.", key, Arrays.toString(grades));
		final Bucket<K, D> newBucket = new Bucket<>(key, delay, unit);
		return buckets.merge(key, newBucket, (a, b) -> {
			a.merge(b);
			return a;
		});
	}

	/**
	 * Try to sift a target.
	 * @param d A target.
	 * @throws IOException while failed to create UUID key for the bucket.
	 * @throws UngradedException while grade is not available but auto grading is not
	 * allowed.
	 * @throws BucketFullException while bucket is full.
	 */
	public void trySift(D d) throws IOException, UngradedException {
		MemPhaser.getInstance().await();
		if (LOG.isDebugEnabled()) {
			LOG.debug("Sift {}.", d);
		}
		final UUID key = Grade.toUUID(d.getGrades());
		final Bucket<K, D> bucket = buckets.computeIfPresent(key, (k, b) -> {
			b.tryAdd(d);
			return b;
		});
		if (bucket == null) {
			if (autoGrading != null) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Auto grading on {} every {} {}.", d,
							autoGrading.getDelay(), autoGrading.getUnit());
				}
				final Bucket<K, D> b = grading(key, autoGrading.getDelay(),
						autoGrading.getUnit(), d.getGrades());
				b.tryAdd(d);
			} else {
				throw new UngradedException();
			}
		}
	}

	/**
	 * Is sifter idle, which means buckets are eventually empty?
	 */
	public boolean isIdle() {
		return !sinking.get() && buckets.isEmpty();
	}

	/**
	 * Is sinking in progress?
	 */
	public boolean isSinking() {
		return sinking.get();
	}

	/**
	 * Collect each entry.
	 */
	<R> void collect(long initialDelay, long delay, TimeUnit unit,
			BiConsumer<K, D> action) {
		// TODO: support multiple collectors ...
		if (this.schedule != null && !this.schedule.isDone()) {
			throw new UnsupportedOperationException(
					"Only one collector can be scheduled.");
		}
		this.schedule = ex.scheduleWithFixedDelay(new Runnable() {

			@Override
			public void run() {
				final List<ForkJoinTask<?>> tasks = buckets.entrySet().parallelStream()
						.filter(e -> e.getValue().isFlippable()).map(e -> {
							return FJP.submit(new SinkTask(e.getKey(), action));
						}).collect(Collectors.toList());
				sinking.set(!tasks.isEmpty());
				tasks.forEach(t -> {
					t.join();
				});
				sinking.set(false);
			}
		}, initialDelay, delay, unit);
		this.down = () -> {
			buckets.forEachValue(Runtime.getRuntime().availableProcessors(), b -> {
				b.sink(action, true);
			});
			buckets.clear();
			return Boolean.TRUE;
		};
	}

	/**
	 * Collect and transform to a result and reduce.
	 *
	 * @param initialDelay Initial delay to perform collect.
	 * @param delay The delay before collect.
	 * @param unit The delay time unit.
	 * @param transformer Transform a target to a result.
	 * @param combiner Combine two results.
	 * @param consumer Consume a result.
	 * @param <R> Type of result.
	 */
	<R> void collect(long initialDelay, long delay, TimeUnit unit,
			Function<D, R> transformer, BinaryOperator<R> combiner,
			final BiConsumer<R, Collection<D>> consumer) {
		// TODO: support multiple collectors ...
		if (this.schedule != null && !this.schedule.isDone()) {
			throw new UnsupportedOperationException(
					"Only one collector can be scheduled.");
		}
		this.schedule = ex.scheduleWithFixedDelay(new Runnable() {

			@Override
			public void run() {
				final List<ForkJoinTask<?>> tasks = buckets.entrySet().parallelStream()
						.filter(e -> e.getValue().isFlippable()).map(e -> {
							return FJP.submit(new ReduceTask<R>(e.getKey(), transformer,
									combiner, consumer));
						}).collect(Collectors.toList());
				sinking.set(!tasks.isEmpty());
				tasks.forEach(t -> {
					t.join();
				});
				sinking.set(false);
			}
		}, initialDelay, delay, unit);
		this.down = () -> {
			buckets.forEachValue(Runtime.getRuntime().availableProcessors(), b -> {
				b.reduce(transformer, combiner, consumer, true);
			});
			buckets.clear();
			return Boolean.TRUE;
		};
	}

	/**
	 * Gracefully shutdown.
	 */
	public void shutdown() {
		LOG.warn("Shutting down, flush all ...");
		this.ex.shutdown();
		// Sink all buckets
		try {
			sinking.set(true);
			final Boolean done = this.down.call();
			if (done != null && done.booleanValue()) {
				LOG.info("All flushed.");
			}
		} catch (Exception e) {
			LOG.error("Unsuccessfull shutdown.", e);
		} finally {
			sinking.set(false);
		}
	}

	/**
	 * Task to reduce a bucket.
	 * @author Chengwei.Yan
	 */
	private final class ReduceTask<R> implements Runnable {

		private final UUID key;
		private final Function<D, R> transformer;
		private final BinaryOperator<R> combiner;
		private final BiConsumer<R, Collection<D>> consumer;

		/**
		 * Constructor
		 */
		private ReduceTask(final UUID key, final Function<D, R> transformer,
				final BinaryOperator<R> combiner,
				final BiConsumer<R, Collection<D>> consumer) {
			this.key = key;
			this.transformer = transformer;
			this.combiner = combiner;
			this.consumer = consumer;
		}

		@Override
		public void run() {
			final Bucket<K, D> b = buckets.remove(key);
			try {
				// Reducing ...
				b.reduce(transformer, combiner, consumer);
			} catch (Exception e) {
				// pour back ...
				buckets.merge(key, b, (b1, b2) -> {
					return b1.merge(b2);
				});
			}
		}

	}

	/**
	 * Task to sink a bucket.
	 * @author Chengwei.Yan
	 */
	private final class SinkTask implements Runnable {

		private final UUID key;
		private final BiConsumer<K, D> action;

		/**
		 * Constructor
		 */
		private SinkTask(final UUID key, final BiConsumer<K, D> action) {
			this.key = key;
			this.action = action;
		}

		@Override
		public void run() {
			System.out.println("Sinking ... " + key);
			final Bucket<K, D> b = buckets.remove(key);
			try {
				// Sinking ...
				b.sink(action);
			} catch (Exception e) {
				// pour back ...
				buckets.merge(key, b, (b1, b2) -> {
					return b1.merge(b2);
				});
			}
		}

	}
}
