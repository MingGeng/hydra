/*
 * Copyright 1999-2011 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jd.bdp.hydra.dubbo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.extension.Activate;
import com.alibaba.dubbo.remoting.TimeoutException;
import com.alibaba.dubbo.rpc.Filter;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.RpcInvocation;
import com.jd.bdp.hydra.BinaryAnnotation;
import com.jd.bdp.hydra.Endpoint;
import com.jd.bdp.hydra.Span;
import com.jd.bdp.hydra.agent.Tracer;
import com.jd.bdp.hydra.agent.support.TracerUtils;

/**
 *
 */
@Activate(group = {Constants.PROVIDER, Constants.CONSUMER})
public class HydraFilter implements Filter {

    private static Logger logger = LoggerFactory.getLogger(HydraFilter.class);

    private Tracer tracer = null;

    // 调用过程拦截
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        
//    	logger.info("HydraFilter#invoke ################### Begin ###################  "+new Date());
    	
    	
    	StringBuilder logStr = new StringBuilder();
    	logStr.append("【平台日志】 - HydraFilter#invoke Parm={invoker=["+invoker+"] - invocation=["+invocation+"]} - ");
    	//异步获取serviceId，没获取到不进行采样
    	String serviceId = null;
        try {
			serviceId = tracer.getServiceId(RpcContext.getContext().getUrl().getServiceInterface());
		} catch (Exception e) {
			logger.error(logStr.toString()+e.getMessage(),e);
			return invoker.invoke(invocation);
		}finally {
			logStr.append("serviceId="+serviceId+" - ");
		}
        
        if (serviceId == null) {
        	logger.warn(logStr.toString()+"未发现服务ID");
            Tracer.startTraceWork();
            return invoker.invoke(invocation);
        }

        long start = System.currentTimeMillis();
        RpcContext context = RpcContext.getContext();
        boolean isConsumerSide = context.isConsumerSide();
        boolean isProviderSide = context.isProviderSide();
        Span span = null;
        Endpoint endpoint = null;
        try {
            //组织EndPoint
        	endpoint = tracer.newEndPoint();
            endpoint.setServiceName(serviceId);
            endpoint.setIp(context.getLocalAddressString());
            endpoint.setPort(context.getLocalPort());
            
            
            logger.debug("HydraFilter#invoke isConsumerSide="+isConsumerSide+",isProviderSide="+isProviderSide+",EndPoint="+endpoint);
            
            if (isConsumerSide) { //是否是消费者
            	logStr.append("[消费端] - ");
                Span span1 = tracer.getParentSpan();
                if (span1 == null) { //为rootSpan
                    span = tracer.newSpan(context.getMethodName(), endpoint, serviceId);//生成root Span
                } else {
                    span = tracer.genSpan(span1.getTraceId(), span1.getId(), tracer.genSpanId(), context.getMethodName(), span1.isSample(), null);
                }
                logStr.append("ParentSpan=["+span1+"] - span=["+span+"]");
            } else if (isProviderSide) {
                Long traceId, parentId, spanId;
                traceId = TracerUtils.getAttachmentLong(invocation.getAttachment(TracerUtils.TID));
                parentId = TracerUtils.getAttachmentLong(invocation.getAttachment(TracerUtils.PID));
                spanId = TracerUtils.getAttachmentLong(invocation.getAttachment(TracerUtils.SID));
                boolean isSample = (traceId != null);
                span = tracer.genSpan(traceId, parentId, spanId, context.getMethodName(), isSample, serviceId);
                logStr.append("[服务端] - span=["+span+"] - ");
            }else{
            	
            }
            invokerBefore(invocation, span, endpoint, start);//记录annotation
            RpcInvocation invocation1 = (RpcInvocation) invocation;
            setAttachment(span, invocation1);//设置需要向下游传递的参数
            Result result = invoker.invoke(invocation);
            logStr.append("result=["+result+"] - ");
            if (result.getException() != null){
            	logger.warn("HydraFilter#invoke fail  result="+result);
                catchException(result.getException(), endpoint);
            }
            return result;
        }catch (RpcException e) {
            if (e.getCause() != null && e.getCause() instanceof TimeoutException){
                catchTimeoutException(e, endpoint);
            }else {
                catchException(e, endpoint);
            }
            logStr.append("Happen Exception ["+e.getMessage()+"] - ");
            logger.error("HydraFilter#invoke Parm is invoker="+invoker+",invocation="+invocation+",serviceId="+serviceId+","+e.getMessage(),e);
            throw e;
        }finally {
            if (span != null) {
                long end = System.currentTimeMillis();
                invokerAfter(invocation, endpoint, span, end, isConsumerSide);//调用后记录annotation
            }
            
//            logger.info("HydraFilter#invoke ################### End ###################  "+new Date()+"\n\n");
            logStr.append("调用完成");
            System.out.println(logStr.toString());
        }
    }

    private void catchTimeoutException(RpcException e, Endpoint endpoint) {
        BinaryAnnotation exAnnotation = new BinaryAnnotation();
        exAnnotation.setKey(TracerUtils.EXCEPTION);
        exAnnotation.setValue(e.getMessage());
        exAnnotation.setType("exTimeout");
        exAnnotation.setHost(endpoint);
        tracer.addBinaryAnntation(exAnnotation);
    }

    private void catchException(Throwable e, Endpoint endpoint) {
        BinaryAnnotation exAnnotation = new BinaryAnnotation();
        exAnnotation.setKey(TracerUtils.EXCEPTION);
        exAnnotation.setValue(e.getMessage());
        exAnnotation.setType("ex");
        exAnnotation.setHost(endpoint);
        tracer.addBinaryAnntation(exAnnotation);
    }

    private void setAttachment(Span span, RpcInvocation invocation) {
        if (span.isSample()) {
            invocation.setAttachment(TracerUtils.PID, span.getParentId() != null ? String.valueOf(span.getParentId()) : null);
            invocation.setAttachment(TracerUtils.SID, span.getId() != null ? String.valueOf(span.getId()) : null);
            invocation.setAttachment(TracerUtils.TID, span.getTraceId() != null ? String.valueOf(span.getTraceId()) : null);
            
            logger.debug("HydraFilter#invoke setAttachment 设置需要向下游传递的参数");
        }
    }

    private void invokerAfter(Invocation invocation, Endpoint endpoint, Span span, long end, boolean isConsumerSide) {
        if (isConsumerSide && span.isSample()) {
            tracer.clientReceiveRecord(span, endpoint, end);
        } else {
            if (span.isSample()) {
                tracer.serverSendRecord(span, endpoint, end);
            }
            tracer.removeParentSpan();
        }
        logger.debug("HydraFilter#invoke invokerAfter ");
    }

    private void invokerBefore(Invocation invocation, Span span, Endpoint endpoint, long start) {
        RpcContext context = RpcContext.getContext();
        logger.debug("HydraFilter#invoke invokerBefore context="+context);
        if (context.isConsumerSide() && span.isSample()) {
            tracer.clientSendRecord(span, endpoint, start);
        } else if (context.isProviderSide()) {
            if (span.isSample()) {
                tracer.serverReceiveRecord(span, endpoint, start);
            }
            tracer.setParentSpan(span);
        }
    }

    //setter
    public void setTracer(Tracer tracer) {
        this.tracer = tracer;
    }

    /*加载Filter的时候加载hydra配置上下文*/
    static {
    	
    	  System.out.println("init HydraFilter *************************************");
//        logger.info("Hydra filter is loading hydra-config file...");
//        String resourceName = "classpath*:/spring/hydra-config.xml";
////        String resourceName = "classpath*:/hydra-config.xml";
//        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[]{
//                resourceName
//        });
//        logger.info("Hydra config context is starting,config file path is:" + resourceName);
//        context.start();
//        logger.info("Hydra config context is started,context is:"+context);
//        
//        try {
//			if(context != null){
//				
//				Tracer tracer = context.getBean(Tracer.class);
//				logger.info("Hydra config context is started,tracer is:"+tracer);
//				
//			}
//		} catch (Throwable e) {
//			// TODO Auto-generated catch block
//			logger.error(e.getMessage(),e);
//		}
//        
    }
}