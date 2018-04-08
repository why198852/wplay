package com.wplay.core.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreaderPool {

	private boolean flag = false;
	private ExecutorService threadPool = null;
	private final int pool_size = 4;

	private ThreaderPool() {
		if (!flag) {
			threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * pool_size);
			flag = true;
		}
	}

	public ExecutorService getThreadPool() {
		return threadPool;
	}

	private static class StaticInnerClass {
		private final static ThreaderPool THREAD_POOL = new ThreaderPool();
	}

	public static ThreaderPool getInstance() {
		return StaticInnerClass.THREAD_POOL;
	}

	public void execute(final Object object) {
		threadPool.execute(new Runnable() {
			public void run() {
				try {

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	public void execute(Runnable runnable) {
		threadPool.execute(runnable);
	}

	public static void main(String[] args) {

		try {
			Integer.parseInt("rr");
		} catch (NumberFormatException e) {
			e.printStackTrace();
			// 下面同理

			StringBuffer sb = new StringBuffer();
			StackTraceElement[] stackTrace = e.getStackTrace();
			sb.append(e.fillInStackTrace());// 需要在e.getStackTrace()之后执行 ，否则装进去之后就抹掉了后面的堆栈异常信息

			for (int i = 0; i < stackTrace.length; i++) {

				sb.append("\r\n");
				sb.append("\tat ");
				sb.append(stackTrace[i]);
			}
			System.err.println(sb.toString());
		}
	}
}
