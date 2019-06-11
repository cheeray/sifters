package com.cheeray.sifters;

import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bucket to hold sifted <code>Gradable</code> targets into a <code>Clone</code> result.
 * @param K Type of key to be graded.
 * @param D Type of raw <code>Gradable</code> target to be graded.
 * @param R Type of <code>Clone</code> sift result.
 * @author Chengwei.Yan
 */
public class Bucket<K, D extends Gradable<K>> {

	private final static Logger LOG = LoggerFactory.getLogger(Bucket.class);

	/** Unique key of the bucket. */
	private final UUID key;
	/** Timeout before flip to sink. */
	private final AtomicLong flipDelay;
	/** Bucket to hold sifted targets. */
	private final ConcurrentHashMap<K, D> bucket;

	/**
	 * Init a bucket with given capacity and flip timeout, results will be converted and
	 * combined.
	 * @param freeMemRatio Max ratio of free memory to be maintained, once exceed, the
	 * thread will be locked.
	 * @param key The bucket key.
	 * @param delay Timeout to flip.
	 * @param unit Unit of timeout value.
	 */
	public Bucket(UUID key, long delay, TimeUnit unit) {
		this.key = key;
		this.flipDelay = new AtomicLong(System.nanoTime() + unit.toNanos(delay));
		this.bucket = new ConcurrentHashMap<>();
	}

	/**
	 * Obtains the bucket key.
	 */
	public UUID getKey() {
		return key;
	}

	/**
	 * Try add a target to bucket. Blocking call if free MEM is under limit.
	 * @param d The target.
	 */
	public void tryAdd(D d) {
		add(d);
	}

	/**
	 * Add a target.
	 * @param d The target.
	 */
	private void add(D d) {
		this.bucket.merge(d.getKey(), d, (ex, ne) -> {
			// Comparing exist and new value ...
			if (ex == null) {
				return ne;
			}
			if (ne == null) {
				return null;
			}
			if (ex.getVersion().compareTo(ne.getVersion()) > 0) {
				return ex;
			} else {
				return ne;
			}
		});
	}

	/**
	 * Is time to flip?
	 */
	public boolean isFlippable() {
		return System.nanoTime() >= flipDelay.get();
	}

	/**
	 * Reduce all targets to a result and pass to consumer.
	 * @param transformer Transform a target to a result.
	 * @param combiner Combine two <code>R</code> results. Constructor
	 * @param consumer The result consumer.
	 */
	public <R> void reduce(Function<D, R> transformer, BinaryOperator<R> combiner,
			BiConsumer<R, Collection<D>> consumer) {
		reduce(transformer, combiner, consumer, false);
	}

	/**
	 * Reduce all targets to a result and pass to consumer.
	 * @param transformer Transform a target to a result.
	 * @param combiner Combine two <code>R</code> results. Constructor
	 * @param consumer The result consumer.
	 * @param forced Force dump all entries.
	 */
	public <R> void reduce(Function<D, R> transformer, BinaryOperator<R> combiner,
			BiConsumer<R, Collection<D>> consumer, boolean forced) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Reduce bucket " + this.key);
		}
		if (transformer == null || combiner == null) {
			throw new UnsupportedOperationException(
					"Not support trasforming result, please provide accumulator and combiner.");
		}
		if (forced || System.nanoTime() >= flipDelay.get()) {
			// Try parallel ...
			final Optional<R> rs = this.bucket.values().parallelStream().map(transformer)
					.reduce(combiner);
			if (rs.isPresent()) {
				consumer.accept(rs.get(), this.bucket.values());
			}
		}
	}

	/**
	 * Sink all targets to a result and pass to consumer.
	 * @see side-effects
	 * @param consumer The
	 */
	void sink(BiConsumer<? super K, ? super D> action) {
		sink(action, false);
	}

	/**
	 * Sink all targets to a result and pass to consumer.
	 * @see side-effects
	 * @param consumer The
	 * @param forced Force dump all entries.
	 */
	void sink(BiConsumer<? super K, ? super D> action, boolean forced) {
		if (forced || System.nanoTime() >= flipDelay.get()) {
			this.bucket.forEach(Runtime.getRuntime().availableProcessors(), action);
		}
	}

	/**
	 * Merge with another bucket.
	 */
	Bucket<K, D> merge(Bucket<K, D> b) {
		b.bucket.forEachValue(Runtime.getRuntime().availableProcessors(), d -> {
			this.add(d);
		});
		return this;
	}
}
