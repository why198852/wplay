package com.wplay.core.util;

import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author James
 * 
 */
public class LogUtil {
	private static final Logger LOG = Logger.getLogger(LogUtil.class);
	private static final Map<Class<?>, Logger> LOG_CACHE = new HashMap<Class<?>, Logger>();

	private static Method LOG_TRACE = null;
	private static Method LOG_DEBUG = null;
	private static Method LOG_INFO = null;
	private static Method LOG_WARN = null;
	private static Method LOG_ERROR = null;
	private static Method LOG_FATAL = null;
	
	static {
		try {
			LOG_TRACE = Logger.class.getMethod("trace", new Class[] { Object.class });
			LOG_DEBUG = Logger.class.getMethod("debug", new Class[] { Object.class });
			LOG_INFO = Logger.class.getMethod("info", new Class[] { Object.class });
			LOG_WARN = Logger.class.getMethod("warn", new Class[] { Object.class });
			LOG_ERROR = Logger.class.getMethod("error", new Class[] { Object.class });
			LOG_FATAL = Logger.class.getMethod("fatal", new Class[] { Object.class });

		} catch (Exception e) {
			LOG.error("Cannot init log methods", e);
		}
	}

	public static Logger getLog(Class<?> clazz) {
		Logger log = (Logger) LOG_CACHE.get(clazz);
		if (log == null) {
			log = Logger.getLogger(clazz);
			LOG_CACHE.put(clazz, log);
		}
		return log;
	}

	public static void info(Class<?> clazz, Object msg) {
		getLog(clazz).info(msg);
	}

	public static void debug(Class<?> clazz, Object msg) {
		getLog(clazz).debug(msg);
	}

	public static void error(Class<?> clazz, Object msg) {
		getLog(clazz).error(msg);
	}

	public static PrintStream getTraceStream(final Logger logger) {
		return getLogStream(logger, LOG_TRACE);
	}

	public static PrintStream getDebugStream(final Logger logger) {
		return getLogStream(logger, LOG_DEBUG);
	}

	public static PrintStream getInfoStream(final Logger logger) {
		return getLogStream(logger, LOG_INFO);
	}

	public static PrintStream getWarnStream(final Logger logger) {
		return getLogStream(logger, LOG_WARN);
	}

	public static PrintStream getErrorStream(final Logger logger) {
		return getLogStream(logger, LOG_ERROR);
	}

	public static PrintStream getFatalStream(final Logger logger) {
		return getLogStream(logger, LOG_FATAL);
	}

	/** Returns a stream that, when written to, adds log lines. */
	private static PrintStream getLogStream(final Logger logger,
			final Method method) {
		return new PrintStream(new ByteArrayOutputStream() {
			private int scan = 0;

			private boolean hasNewline() {
				for (; scan < count; scan++) {
					if (buf[scan] == '\n')
						return true;
				}
				return false;
			}

			public void flush() throws IOException {
				if (!hasNewline())
					return;
				try {
					method.invoke(logger, new String[] { toString().trim() });
				} catch (Exception e) {
					LOG.error("Cannot log with method [" + method + "]", e);
				}
				reset();
				scan = 0;
			}
		}, true);
	}

}