package com.jd.bdp.hydra.agent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class TestMain {
	 private static Logger logger = LoggerFactory.getLogger(TestMain.class);

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		logger.info("############# Hydra filter is loading hydra-config file...############# ");
        String resourceName = "classpath:hydra-config.xml";
//        String resourceName = "hydra-config.xml";
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[]{
                resourceName
        });
        logger.info("############# Hydra config context is starting,config file path is:" + resourceName+"#############");
        context.start();
        
        
        
	}

}
