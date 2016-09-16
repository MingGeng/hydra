package com.jd.bdp.hydra.agent.support;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.assembler.AutodetectCapableMBeanInfoAssembler;

import com.jd.bdp.hydra.Span;
import com.jd.bdp.hydra.agent.SyncTransfer;

/**
 * Date: 13-3-19
 * Time: 下午6:26
 * 异步发送实现类
 */
public class DefaultSyncTransfer implements SyncTransfer {

    private static Logger logger = LoggerFactory.getLogger(DefaultSyncTransfer.class);

    private ArrayBlockingQueue<Span> queue;

    private ScheduledExecutorService executors = null;
    private List<Span> spansCache;


    //serviceName isReady
//    private volatile boolean isReady = false; //是否获得种子等全局注册信息
    private AtomicBoolean isReady = new AtomicBoolean(false);

    private ConcurrentHashMap<String, Boolean> isServiceReady = new ConcurrentHashMap<String, Boolean>();

    private GenerateTraceId generateTraceId = new GenerateTraceId(0L);

    private TraceService traceService;

    private Long flushSize;

    private Long waitTime;

    private TransferTask task;

    private String applicationName = "test";

    @Override
    public void setTraceService(TraceService traceService) {
        this.traceService = traceService;
    }

    public DefaultSyncTransfer(Configuration c) {
        this.flushSize = c.getFlushSize() == null ? 1024L : c.getFlushSize();
        this.waitTime = c.getDelayTime() == null ? 60000L : c.getDelayTime();
        this.queue = new ArrayBlockingQueue<Span>(c.getQueueSize());
        this.spansCache = new ArrayList<Span>();
        this.executors = Executors.newSingleThreadScheduledExecutor();
        this.task = new TransferTask();
        this.applicationName = c.getApplicationName() == null?"test":c.getApplicationName();
        
        System.out.println("init transfer Configuration="+c);
    }

    @Override
    public String appName() {
        //fixme
        return applicationName;
    }

    private class TransferTask extends Thread {
        
    	private boolean runFlag = true;
    	private boolean isActiveTask = false;
    	
    	TransferTask() {
            this.setName("TransferTask-Thread");
        }
    	
        @Override
        public void run() {
        	isActiveTask = true;
        	System.out.println("【平台日志】 - 【后台消息发送线程】 - "+this.getId()+" - 启动 ............");
            while (runFlag) {
            	StringBuilder sb = new StringBuilder();
            	sb.append("【平台日志】 - 【后台消息发送线程】 - "+this.getId()+" - 运行 - ");
                try {
                	sb.append("isReady=["+isReady()+"] - ");
                    if (!isReady()) {//重试直到注册成功
                        //全局信息网络注册，输入流：应用名 @ 输出流：包含种子的Map对象
                        boolean r = traceService.registerService(appName(), new ArrayList<String>());
                        sb.append("全局信息网络注册["+r+"] - ");
                        if (r) {
                            generateTraceId = new GenerateTraceId(traceService.getSeed());
//                            isReady = true;
                            isReady.set(true);
                        } else {
//                            synchronized (this) {
//                                this.wait(waitTime);
//                            }
                        	try{Thread.sleep(3000);}catch(Exception e){e.printStackTrace();}
                            
                        }
                    } else {
//                    	sb.append("task.isInterrupted=["+task.isInterrupted()+"] - ");
                    	 //检查是否有未注册服务，先注册
                        for (Map.Entry<String, Boolean> entry : isServiceReady.entrySet()) {
                        	sb.append(entry+" - ");
                            if (false == entry.getValue()) {//没有注册，先注册
                                boolean r = traceService.registerService(appName(), entry.getKey());
                                if (r) {
                                    entry.setValue(true);
                                }
                                System.out.println("【平台日志】 - 【后台消息发送线程】 - "+this.getId()+" - 注册服务 - "+entry+" - 结果["+r+"]");
                            }
                        }
                        //-----------------------------
//                        Span first = queue.poll();
                        if(!queue.isEmpty()){
//                        	spansCache.add(first);
                            queue.drainTo(spansCache);
                            traceService.sendSpan(spansCache);
                            sb.append("send span size=["+spansCache.size()+"] - ");
                            spansCache.clear();
                        }
                        
                    }
                    try{Thread.sleep(1000);}catch(Exception e){e.printStackTrace();}
                    
                } catch (Throwable e) {
                    e.printStackTrace();
                    logger.info(e.getMessage());
                    sb.append("exception=["+e.getMessage()+"] - ");
                    e.printStackTrace();
                }finally {
                	sb.append(new Date());
                	logger.debug(sb.toString());
				}
            }
            System.out.println("【平台日志】 - 【后台消息发送线程】 - "+this.getId()+" - 终止 ............");
        }
        
        
        public synchronized void stopTask(){
        	System.out.println("【平台日志】 - 【后台消息发送线程】 - "+this.getId()+" - 停止 ............");
    		runFlag = false;
    		isActiveTask = false;
    	}
        
        public synchronized boolean isActiveTask(){
        	return isActiveTask;
        }
    }

    @Override
    public boolean isReady() {
        return isReady.get();
    }

    @Override
    public boolean isServiceReady(String serviceName) {
        if (serviceName != null && isServiceReady.containsKey(serviceName))
            return isServiceReady.get(serviceName);
        else
            return false;
    }

    @Override
    public void syncSend(Span span) {
        try {
            queue.add(span);
        } catch (Exception e) {
            logger.info(" span : ignore ..");
        }
    }

    @Override
    public void start() throws Exception {
    	System.out.println("【平台日志】 - DefaultSyncTransfer#start 开始启动后台消息发送线程.................. ");
    	System.out.println("【平台日志】 - DefaultSyncTransfer#start traceService=["+traceService+"] - task.isActiveTask=["+task.isActiveTask()+"]");
        if (traceService != null && !task.isActiveTask()) {
            task.start();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    cancel();
                }
            });
            System.out.println("【平台日志】 - DefaultSyncTransfer#start 启动后台消息发送线程[成功]");
        } else if (traceService == null) {
        	System.out.println("【平台日志】 - DefaultSyncTransfer#start 启动后台消息发送线程[失败] traceService = null ");
            throw new Exception("TraceServie is null.can't starting SyncTransfer");
        }
    }

    public void cancel() {
    	System.out.println("【平台日志】 - DefaultSyncTransfer#start [终止] 后台消息发送线程 ................ ");
        task.stopTask();
    }

    @Override
    public String getServiceId(String name) {
        String serviceId = null;
        serviceId = traceService.getServiceId(name);
        //可能是未注册的服务
        if (null == serviceId) {
        	System.out.println("【平台日志】 - DefaultSyncTransfer#getServiceId name=["+name+"] 可能是未注册的服务-->设置未注册标志，交给task去注册");
            isServiceReady.putIfAbsent(name, false);//设置未注册标志，交给task去注册
        }
        return serviceId;
    }

    @Override
    public Long getTraceId() {
        return generateTraceId.getTraceId();
    }
    @Override
    public Long getSpanId() {
        return generateTraceId.getTraceId();
    }
}
