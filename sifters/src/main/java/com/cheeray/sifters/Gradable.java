package com.cheeray.sifters;

import java.time.Instant;

/**
 * Can be graded, identified and versionable.
 * @param K Type of identity key.
 * @author Chengwei.Yan
 */
public interface Gradable<K> {

	/**
	 * Obtain grades.
	 */
	public Grade<?>[] getGrades();

	/**
	 * Obtain version.
	 */
	public Instant getVersion();

	/**
	 * Obtain the unique key.
	 */
	public K getKey();

}
