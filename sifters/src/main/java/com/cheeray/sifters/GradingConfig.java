package com.cheeray.sifters;

import java.util.concurrent.TimeUnit;

/** Grading configuration. */
final class GradingConfig {
	private final Grade<?>[] grades;
	private final long delay;
	private final TimeUnit unit;

	/**
	 * Constructor
	 */
	GradingConfig(long delay, TimeUnit unit, Grade<?>... grades) {
		this.grades = grades;
		this.delay = delay;
		this.unit = unit;
	}

	public Grade<?>[] getGrades() {
		return grades;
	}

	public long getDelay() {
		return delay;
	}

	public TimeUnit getUnit() {
		return unit;
	}
}