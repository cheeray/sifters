package com.cheeray.sifters;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Phaser of free memory ratio which block all waiters once the ratio is under configured.
 * <p>
 * Set system property <code>"sifter.mem.free.percent"</code> for free memory ratio,
 * default 30.
 * </p>
 * <p>
 * Set system property <code>"sifter.mem.period.mills"</code> for memory checking
 * frequency in milliseconds, default 500.
 * </p>
 * @author Chengwei.Yan
 */
public class MemPhaser {

	/** Internal instance holder. */
	private static class MemLockHolder {
		private static MemPhaser INSTANCE = new MemPhaser();
	}

	/**
	 * Obtains an instance.
	 */
	public static MemPhaser getInstance() {
		return MemLockHolder.INSTANCE;
	}

	/** Memory checking scheduler. */
	private final ScheduledExecutorService ex;
	/** Phaser. */
	private final AtomicReference<Phaser> phaser;
	
	private final AtomicBoolean gc;

	/**
	 * Constructor
	 */
	private MemPhaser() {
		this.phaser = new AtomicReference<>();
		this.gc = new AtomicBoolean(false);
		this.ex = Executors.newSingleThreadScheduledExecutor();
		this.ex.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				final BigDecimal act = BigDecimal
						.valueOf(Runtime.getRuntime().freeMemory() * 100l)
						.divide(BigDecimal.valueOf(Runtime.getRuntime().totalMemory()), 2,
								RoundingMode.DOWN);
				/** Ratio of free MEM, used to restrict bucket capacity. */
				final BigDecimal ratio = new BigDecimal(
						System.getProperty("sifter.mem.free.percent", "30"));
				if (act.compareTo(ratio) <= 0) {
					// Block ...
					phaser.compareAndSet(null, new Phaser(1));
					if (Boolean
							.parseBoolean(System.getProperty("sifter.mem.gc", "false"))) {
						if(gc.compareAndSet(false, true)){
							System.gc();
						}
					}
				} else {
					gc.compareAndSet(true, false);
					// Release ...
					final Phaser p = phaser.getAndSet(null);
					if (p != null && !p.isTerminated()) {
						p.forceTermination();
					}
				}
			}
		}, 0, Integer.parseInt(System.getProperty("sifter.mem.period.mills", "500")),
				TimeUnit.MILLISECONDS);
	}

	/**
	 * Await on the memory ratio phase.
	 */
	public void await() {
		final Phaser ph = phaser.getAndUpdate(p -> {
			if (p != null && !p.isTerminated()) {
				p.register();
			}
			return p;
		});
		if (ph != null) {
			ph.arriveAndAwaitAdvance();
		}
	}
}
