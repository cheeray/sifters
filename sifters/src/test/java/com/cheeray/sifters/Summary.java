package com.cheeray.sifters;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

public class Summary {

	private final String a;
	private final String b;
	private final String c;
	private final int d;
	private AtomicInteger e;
	private AtomicInteger f;
	private Instant version;

	Summary(Product p) {
		this.a = p.getA();
		this.b = p.getB();
		this.c = p.getC();
		this.d = p.getD();
		this.e = new AtomicInteger(p.getE());
		this.f = new AtomicInteger(p.getF());
		this.version = p.getVersion();
	}
	
	Summary(String a, String b, String c, int d, int e, int f) {
		this.a = a;
		this.b = b;
		this.c = c;
		this.d = d;
		this.e = new AtomicInteger(e);
		this.f = new AtomicInteger(f);
		this.version = Instant.now();
	}

	public Summary clone(){
		return new Summary(a,b,c,d,e.get(),f.get());
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

	public Instant getVersion() {
		return version;
	}

	public AtomicInteger getE() {
		return e;
	}

	public void addE(int e) {
		this.e.addAndGet(e);
	}

	public AtomicInteger getF() {
		return f;
	}

	public void addF(int f) {
		this.f.addAndGet(f);
	}
}
