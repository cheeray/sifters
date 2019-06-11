package com.cheeray.sifters;

import java.time.Instant;

import com.cheeray.sifters.Gradable;
import com.cheeray.sifters.Grade;

public class Product implements Gradable<String> {

	private final String pku;
	private final String a;
	private final String b;
	private final String c;
	private final int d;
	private int e;
	private int f;
	private Instant version;

	Product(String pku, String a, String b, String c, int d, int e, int f, Instant version) {
		this.pku = pku;
		this.a = a;
		this.b = b;
		this.c = c;
		this.d = d;
		this.e = e;
		this.f = f;
		this.version = version;
	}

	@Override
	public Instant getVersion() {
		return version;
	}

	@Override
	public String getKey() {
		return pku;
	}

	@Override
	public Grade<?>[] getGrades() {
		return Grade.from(a, b, c, d);
	}

	public String getA() {
		return a;
	}

	public String getB() {
		return b;
	}

	public String getC() {
		return c;
	}

	public int getD() {
		return d;
	}

	public int getE() {
		return e;
	}

	public void setE(int e) {
		this.e = e;
	}

	public int getF() {
		return f;
	}

	public void setF(int f) {
		this.f = f;
	}
}
