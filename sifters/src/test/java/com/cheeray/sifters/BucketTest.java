package com.cheeray.sifters;

import org.testng.annotations.Test;
import org.testng.util.TimeUtils;

import com.cheeray.sifters.Bucket;
import com.cheeray.sifters.Gradable;
import com.cheeray.sifters.Grade;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.DataProvider;

public class BucketTest {
	@Test(dataProvider = "dp")
	public void f(Integer n, String s) {
		
	}

	@DataProvider(name = "init")
	public Object[][] dp() {
		return new Object[][] {
				new Object[] {
						UUID.randomUUID(), 0, TimeUnit.SECONDS
				}, new Object[] {
						UUID.randomUUID(), -1, TimeUnit.SECONDS
				}, new Object[] {
						UUID.randomUUID(), 100, TimeUnit.SECONDS
				}, new Object[] {
						UUID.randomUUID(), -100, TimeUnit.SECONDS
				}, new Object[] {
						null, 1l, TimeUnit.SECONDS
				},
		};
	}

	@Test(dataProvider = "init")
	public void Bucket(UUID key, long delay, TimeUnit unit) throws IOException {
		final Bucket b = new Bucket(key, delay, unit);
		System.out.println(System.getProperty("java.io.tmpdir"));
		Assert.assertNotNull(b);
	}

	@Test
	public void add() {
		final Bucket b = new Bucket(UUID.randomUUID(), 10, TimeUnit.SECONDS);
		Gradable d = new Gradable<String>() {

			@Override
			public Grade<?>[] getGrades() {
				// TODO implement Gradable<String>.getGrades
				return Grade.from("A");
			}

			@Override
			public Instant getVersion() {
				// TODO implement Gradable<String>.getVersion
				return Instant.now();
			}

			@Override
			public String getKey() {
				return UUID.randomUUID().toString();
			}
		};
		b.tryAdd(d);
	}

	@Test
	public void getKey() {
		throw new RuntimeException("Test not implemented");
	}

	@Test
	public void isFlippable() {
		throw new RuntimeException("Test not implemented");
	}

	@Test
	public void merge() {
		throw new RuntimeException("Test not implemented");
	}

	@Test
	public void reduceFunctionDRBinaryOperatorRBiConsumerRCollectionD() {
		throw new RuntimeException("Test not implemented");
	}

	@Test
	public void reduceFunctionDRBinaryOperatorRBiConsumerRCollectionDboolean() {
		throw new RuntimeException("Test not implemented");
	}

	@Test
	public void sinkBiConsumersuperKsuperD() {
		throw new RuntimeException("Test not implemented");
	}

	@Test
	public void sinkBiConsumersuperKsuperDboolean() {
		throw new RuntimeException("Test not implemented");
	}

	@Test
	public void tryAdd() {
		throw new RuntimeException("Test not implemented");
	}
}
