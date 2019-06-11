package com.cheeray.sifters;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;

/**
 * Builder of sifter.
 * <p>
 * Set system property <code>"sifter.collect.delay"</code> for the delay after each
 * collect. And <code>"sifter.collect.delay.unit"</code> for the time unit of the delay.
 * </p>
 * 
 * @author Chengwei.Yan
 */
public final class SiftBuilder {

	private final int initialBuckets;
	private final float loadFactor;
	private final int concurrencyLevel;
	private final Map<UUID, GradingConfig> grades = new HashMap<>();
	private GradingConfig autoGrading = null;

	/**
	 * Constructor
	 */
	public SiftBuilder(int initialBuckets, float loadFactor, int concurrencyLevel) {
		this.initialBuckets = initialBuckets;
		this.loadFactor = loadFactor;
		this.concurrencyLevel = concurrencyLevel;
	}

	/**
	 * Is auto grading allowed?
	 */
	public SiftBuilder autoGrading() {
		final long delay = Long
				.parseLong(System.getProperty("sifter.collect.delay", "1000"));
		final TimeUnit unit = TimeUnit
				.valueOf(System.getProperty("sifter.collect.delay.unit", "MILLISECONDS"));
		return autoGrading(delay, unit);
	}

	/**
	 * Is auto grading allowed?
	 */
	public SiftBuilder autoGrading(long delay, TimeUnit unit) {
		this.autoGrading = new GradingConfig(delay, unit);
		return this;
	}

	/**
	 * Grading on given grades. Default delay 1 second if system property
	 * "sifter.collect.delay" was not set.
	 * @param gs Grades are allowed.
	 */
	public SiftBuilder grading(Grade<?>... gs) throws IOException {
		final long delay = Long
				.parseLong(System.getProperty("sifter.collect.delay", "1000"));
		final TimeUnit unit = TimeUnit
				.valueOf(System.getProperty("sifter.collect.delay.unit", "MILLISECONDS"));
		return grading(delay, unit, gs);
	}

	/**
	 * Grading with given capacity.
	 * 
	 */
	public SiftBuilder grading(long delay, TimeUnit unit, Grade<?>... gs)
			throws IOException {
		grades.put(Grade.toUUID(gs), new GradingConfig(delay, unit, gs));
		return this;
	}

	/**
	 * Collect sift results.
	 * @return a sifter.
	 */
	public <K, D extends Gradable<K>> Sifter<K, D> collect(long initialDelay, long delay,
			TimeUnit unit, BiConsumer<K, D> action) {
		if (action == null)
			throw new IllegalArgumentException("Missing action.");
		final Sifter<K, D> sifter = new Sifter<>(initialBuckets, loadFactor,
				concurrencyLevel, autoGrading);
		grades.forEach((k, c) -> {
			sifter.grading(k, c);
		});
		sifter.collect(initialDelay, delay, unit, action);
		return sifter;
	}

	/**
	 * Reduce and consume sift results by transforming to a result.
	 */
	public <K, D extends Gradable<K>, R> Sifter<K, D> reduce(long initialDelay,
			long delay, TimeUnit unit, Function<D, R> transformer,
			BinaryOperator<R> combiner, final BiConsumer<R, Collection<D>> consumer) {
		if (transformer == null || combiner == null || consumer == null)
			throw new IllegalArgumentException(
					"Missing transformer, combiner or consumer.");
		final Sifter<K, D> sifter = new Sifter<>(initialBuckets, loadFactor,
				concurrencyLevel, autoGrading);
		grades.forEach((k, c) -> {
			sifter.grading(k, c);
		});
		sifter.collect(initialDelay, delay, unit, transformer, combiner, consumer);
		return sifter;
	}
}