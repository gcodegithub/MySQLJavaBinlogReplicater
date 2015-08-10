package cn.ce.utils;

import javax.servlet.ServletContext;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.web.context.WebApplicationContext;

public class SpringUtil implements ApplicationContextAware {

	private static WebApplicationContext applicationContext;

	public void setApplicationContext(ApplicationContext appCtx)
			throws BeansException {
		applicationContext = (WebApplicationContext) appCtx;
		ServletContext sc = applicationContext.getServletContext();

	}

	public static Object getBean(String name) {
		return applicationContext.getBean(name);
	}

}
