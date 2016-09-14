package com.jd.bdp.hydra.dubbomonitor;


import java.util.List;
import java.util.Map;

public interface LeaderService {
    
    public Map<String,String> registerClient(String name,List<String> services);
    
    public String registerClient(String name,String service);

}