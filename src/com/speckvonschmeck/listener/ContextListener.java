package com.speckvonschmeck.listener;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.speckvonschmeck.spark.SpectrumJob;

public class ContextListener implements ServletContextListener {

	@Override
	public void contextDestroyed(ServletContextEvent arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void contextInitialized(ServletContextEvent arg0) {
		// TODO Auto-generated method stub
		System.out.println("Server started!");

		SpectrumJob.runSparkJob();
//		new Thread(new SpectrumConsumer()).start();
	}


}
