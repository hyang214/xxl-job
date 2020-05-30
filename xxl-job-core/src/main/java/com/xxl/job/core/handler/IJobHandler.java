package com.xxl.job.core.handler;

import com.xxl.job.core.biz.model.ReturnT;

import java.lang.reflect.InvocationTargetException;

/**
 * job handler
 *
 * + 抽象任务处理类
 * 		+ 包含3个场景方法
 * 			+ 执行：核心执行方法，必须实现
 * 			+ 初始化：可以不处理，有默认空实现
 * 			+ 消耗：	可以不处理，有默认空实现
 * 		+ 三个常见返回
 * 			+ 成功
 * 			+ 失败
 * 			+ 超时失败
 *
 * @author xuxueli 2015-12-19 19:06:38
 */
public abstract class IJobHandler {


	/** success */
	public static final ReturnT<String> SUCCESS = new ReturnT<String>(200, null);
	/** fail */
	public static final ReturnT<String> FAIL = new ReturnT<String>(500, null);
	/** fail timeout */
	public static final ReturnT<String> FAIL_TIMEOUT = new ReturnT<String>(502, null);


	/**
	 * execute handler, invoked when executor receives a scheduling request
	 *
	 * @param param
	 * @return
	 * @throws Exception
	 */
	public abstract ReturnT<String> execute(String param) throws Exception;


	/**
	 * init handler, invoked when JobThread init
	 */
	public void init() throws InvocationTargetException, IllegalAccessException {
		// do something
	}


	/**
	 * destroy handler, invoked when JobThread destroy
	 */
	public void destroy() throws InvocationTargetException, IllegalAccessException {
		// do something
	}


}
