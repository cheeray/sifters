package com.cheeray.sifters;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.UUID;

/**
 * Define a grade for a target.
 * @param T type of target.
 * @author Chengwei.Yan
 */
public class Grade<T> implements Comparable<Grade<T>>, Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * Unique ID for a group of grades.
	 * @param grades The group of grades.
	 */
	public static UUID toUUID(Grade<?>... grades) throws IOException {
		Arrays.sort(grades);
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
			try (ObjectOutputStream os = new ObjectOutputStream(bos)) {
				for (Grade<?> g : grades) {
					os.writeInt(g.getLevel());
					os.writeObject(g.getT());
				}
				os.flush();
			}
			return UUID.nameUUIDFromBytes(bos.toByteArray());
		}
	}

	/**
	 * Grading from given specs.
	 * @param ts Ordered specs to be used for grading.
	 */
	public static <T> Grade<?>[] from(T... ts) {
		final Grade<?>[] grades = new Grade[ts.length];
		int i = 0;
		for (T t : ts) {
			grades[i] = new Grade<>(i, t);
			i++;
		}
		return grades;
	}

	/** Grade level. */
	private final int level;

	/** Spec. */
	private final T t;

	private Grade(int level, T t) {
		this.level = level;
		this.t = t;
	}

	public int getLevel() {
		return level;
	}

	public T getT() {
		return t;
	}

	@Override
	public int compareTo(Grade<T> o) {
		return level - o.level;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + level;
		result = prime * result + ((t == null) ? 0 : t.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Grade<?> other = (Grade<?>) obj;
		if (level != other.level)
			return false;
		if (t == null) {
			if (other.t != null)
				return false;
		} else if (!t.equals(other.t))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Grade-" + level + ":" + t;
	}

}
