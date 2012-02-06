package com.opencredo.sandbox.gawain.springbatch.remote.chunking;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * super class with common methods for starting a new thread
 * 
 * @author gawain
 *
 * @param <V>
 */
public abstract class AbstractStartable<V> implements Callable<V> {

	Future<V> future;
	private final ExecutorService executorService = Executors.newFixedThreadPool(1);
	
	public void shutdown() {
		executorService.shutdown();
	}
	
	public AbstractStartable start() {
		future = executorService.submit((Callable) this);
		return this;
	}

	public Future<V> getFuture() {
		return future;
	}
	
}
