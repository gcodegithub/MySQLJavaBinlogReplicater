/*
 * Copyright (c) 2010-2013 300.cn All Rights Reserved
 *
 * File:ThreadPoolUtils.java Project: DVS_Dev
 * 
 * Creator:高蓬 
 * Date:2014-1-26 下午4:50:04
 * 
 */
package cn.ce.binlog.mysql.util;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import cn.ce.cons.Const;

public class ThreadPoolUtils {
	private static ExecutorService	executor			= Executors.newFixedThreadPool(5);
	private static ExecutorService	consumerExecutor	= Executors.newFixedThreadPool(Const.poolSize);

	public static void doBuzzToExePool(Callable callable) throws Throwable {
		@SuppressWarnings("rawtypes")
		FutureTask task = new FutureTask(callable);
		executor.submit(task);
	}

	public static void doConsumeBuzzToExePool(Callable callable) throws Throwable {
		@SuppressWarnings("rawtypes")
		FutureTask task = new FutureTask(callable);
		consumerExecutor.submit(task);
	}
}
