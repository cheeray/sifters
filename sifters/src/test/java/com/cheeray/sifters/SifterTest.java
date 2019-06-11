package com.cheeray.sifters;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.util.Strings;

import com.cheeray.sifters.Sifter;
import com.cheeray.sifters.UngradedException;

public class SifterTest {

	private Product[] products;

	@BeforeClass
	public void beforeClass() {
		products = new Product[1000];
		final Instant now = Instant.now();
		for (int i = 1; i <= 1000; i++) {
			products[i - 1] = new Product("P" + i, "A" + i % 2, "B" + i % 2, "C" + i % 2,
					i % 5, i % 5 * 100, i % 5 * 1000, now);
			// products[i-1] = new Product("P" + i, "A1", "B1", "C1", 1, i * 100,
			// i * 1000);
		}
		System.setProperty("sifter.bucket.size", "10000");
	}

	@AfterClass
	public void afterClass() {
		products = null;
		System.clearProperty("sifter.bucket.size");
	}

	@Test(threadPoolSize=5, invocationCount=10)
	public void sifter() throws InterruptedException {
		final long start = System.nanoTime();
		final ConcurrentHashMap<Integer, Integer> totalEs = new ConcurrentHashMap<>();
		final ConcurrentHashMap<Integer, Integer> totalFs = new ConcurrentHashMap<>();
		final ConcurrentHashMap<String, Instant> versions = new ConcurrentHashMap<>();
		BinaryOperator<Summary> combiner = (a, b) -> {
			a.addE(b.getE().get());
			a.addF(b.getF().get());
			return a;
		};

		try {
			final Sifter<String, Product> s = siftProducts(totalEs, totalFs, versions);
			while (!s.isIdle()) {
				Thread.currentThread().sleep(100);
			}
			Assert.assertEquals(1000, versions.size());
			
			totalEs.forEach((k, v) -> {
				System.out.println("" + k + " : " + v.intValue());
				Assert.assertEquals(v.intValue(), k * 20000);
			});
			totalFs.forEach((k, v) -> {
				Assert.assertEquals(v.intValue(), k * 200000);
			});
			System.out.println("Took: " + (System.nanoTime() - start));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Test
	public void shutdown() throws InterruptedException {
		final long start = System.nanoTime();
		final ConcurrentHashMap<Integer, Integer> totalEs = new ConcurrentHashMap<>();
		final ConcurrentHashMap<Integer, Integer> totalFs = new ConcurrentHashMap<>();
		final ConcurrentHashMap<String, Instant> versions = new ConcurrentHashMap<>();
		try {
			final Sifter<String, Product> s = siftProducts(totalEs, totalFs, versions);
			s.shutdown();
			while (!s.isIdle()){
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					Assert.assertNull(e);
				}
			}
			Assert.assertEquals(versions.size(), 1000);
			
			totalEs.forEach((k, v) -> {
				System.out.println("" + k + " : " + v.intValue());
				Assert.assertEquals(v.intValue(), k * 20000);
			});
			totalFs.forEach((k, v) -> {
				Assert.assertEquals(v.intValue(), k * 200000);
			});
			System.out.println("Took: " + (System.nanoTime() - start));
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	private Sifter<String, Product> siftProducts(
			final ConcurrentHashMap<Integer, Integer> totalEs,
			final ConcurrentHashMap<Integer, Integer> totalFs,
			final ConcurrentHashMap<String, Instant> versions) {
		final Sifter<String, Product> s = Sifter.sift(1000, 0.2f, 5).autoGrading()
				.collect(0, 1, TimeUnit.SECONDS, (k, d) -> {
					System.out.println(
							"Pro: " + k + ": " + Arrays.toString(d.getGrades()) + " : " + d.getE());
					final Instant exist = versions.get(k);
					if (exist!= null){
						if (exist.isAfter(d.getVersion())){
							totalEs.compute(d.getD(), (i,v)->{return v!=null?(v.intValue()+d.getE()):d.getE();});
							totalFs.compute(d.getD(), (i,v)->{return v!=null?(v.intValue()+d.getF()):d.getF();});
						} else {
							System.out.println("-------- OLD ---" + k);
						}
					} else {
						if (totalEs.compute(d.getD(), (i,v)->{return v!=null?(v.intValue()+d.getE()):d.getE();}) == null){
							if (totalEs.putIfAbsent(d.getD(), d.getE()) == null){
								throw new RuntimeException("No way to sum up E for " + k);
							}
						}
						if (totalFs.compute(d.getD(), (i,v)->{return v!=null?(v.intValue()+d.getF()):d.getF();}) == null){
							if(totalFs.putIfAbsent(d.getD(), d.getF()) == null){
								throw new RuntimeException("No way to sum up F for " + k);
							}
						}
						versions.putIfAbsent(k,d.getVersion());
					}
				}/*, d -> {
					return new Summary(d);
				}, combiner, sum -> {

					System.out.println(Strings.join(":", new String[] {
							sum.getA(), sum.getB(), sum.getC(), sum.getD() + "",
							sum.getE().get() + "", sum.getF().get() + ""
					}));
//						totalEs.putIfAbsent(sum.getD(), 0);
//						totalFs.putIfAbsent(sum.getD(), 0);
					totalEs.compute(sum.getD(), (k,v)->{return v!=null?(v+ sum.getE().get()):sum.getE().get();});
					totalFs.compute(sum.getD(), (k,v)->{return v!=null?(v+ sum.getF().get()):sum.getF().get();});
				}*/);
		final Runnable task = new Runnable() {

			@Override
			public void run() {
				Arrays.stream(products).parallel().forEach(p -> {
					try {
						s.trySift(p);

					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (UngradedException e) {
						e.printStackTrace();
					} 
				});
			}

		};

		final ExecutorService ex = Executors.newCachedThreadPool();
		List<Future<?>> tasks = IntStream.range(1, 10).mapToObj(i -> {
			return ex.submit(task);
		}).collect(Collectors.toList());
		tasks.forEach(f -> {
			try {
				f.get();
			} catch (InterruptedException | ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
		return s;
	}
}
