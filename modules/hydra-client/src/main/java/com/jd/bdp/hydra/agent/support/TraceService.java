package com.jd.bdp.hydra.agent.support;


import com.jd.bdp.hydra.Span;
import com.jd.bdp.hydra.agent.CollectorService;
import com.jd.bdp.hydra.agent.RegisterService;
import com.jd.bdp.hydra.dubbomonitor.HydraService;
import com.jd.bdp.hydra.dubbomonitor.LeaderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Date: 13-3-27
 * Time: 上午10:57
 */
public class TraceService implements RegisterService, CollectorService {

    private static final Logger logger = LoggerFactory.getLogger(TraceService.class);

    private LeaderService leaderService;
    private HydraService hydraService;
    private Map<String, String> registerInfo;
    public static final String APP_NAME = "applicationName";
    public static final String SEED = "seed";
    private boolean isRegister = false;

    public boolean isRegister() {
        return isRegister;
    }

    @Override
    public void sendSpan(List<Span> spanList) {
        //fixme try-catch性能影响？
        try {
            hydraService.push(spanList);
        } catch (Exception e) {
            logger.warn("Trace data push failure~ spanList="+spanList,e);
        }
    }

    @Override
    public boolean registerService(String name, List<String> services) {
//        logger.info("TraceService#registerService "+name + " " + services);
        try {
            this.registerInfo = leaderService.registerClient(name, services);
        } catch (Exception e) {
        	System.out.println("【平台日志】 - Fail to invoke TraceService#registerService - name=["+name+"] - services=["+services+"] - exception=["+e.getMessage()+"]");
            logger.error("[Hydra] Client global config-info cannot regist into the hydra system",e);
            logger.warn("[Hydra] Client global config-info cannot regist into the hydra system",e);
        }
        if (registerInfo != null) {
            logger.info("[Hydra] Global registry option is ok!");
            isRegister = true;
        }
        
        System.out.println("【平台日志】 - TraceService#registerService appName=["+name + "] - services=[" + services+"] - result=["+isRegister+"]["+registerInfo+"]");
        
        return isRegister;
    }

    /*更新注册信息*/
    @Override
    public boolean registerService(String appName, String serviceName) {
//        logger.info("TraceService#registerService "+appName + " " + serviceName);
    	String serviceId = null;
    	boolean result = false;
        try {
			try {
			    serviceId = leaderService.registerClient(appName, serviceName);
			} catch (Exception e) {
			    logger.warn("[Hydra] client cannot regist service <" + serviceName + "> into the hydra system",e);
			}
			if (serviceId != null) {
			    logger.info("[Hydra] Registry ["+serviceName+"] option is ok!");
			    registerInfo.put(serviceName, serviceId); //更新本地注册信息
			    result = true;
			} else{
				result = false;
			}
			return result;
		} finally{
			System.out.println("【平台日志】 - TraceService#registerService - 更新注册信息 - appName=["+appName + "] - serviceName=[" + serviceName+"] - result=["+result+"]["+serviceId+"]");
		}
    }

    public LeaderService getLeaderService() {
        return leaderService;
    }

    public void setLeaderService(LeaderService leaderService) {
        this.leaderService = leaderService;
    }

    public HydraService getHydraService() {
        return hydraService;
    }

    public void setHydraService(HydraService hydraService) {
        this.hydraService = hydraService;
    }

    public String getServiceId(String service) {
        if (isRegister && registerInfo.containsKey(service))
            return registerInfo.get(service);
        else
            return null;
    }

    public Long getSeed() {
        String s = null;
        if (isRegister) {
            s = registerInfo.get(SEED);
            return Long.valueOf(s);
        }
        return null;
    }
}
