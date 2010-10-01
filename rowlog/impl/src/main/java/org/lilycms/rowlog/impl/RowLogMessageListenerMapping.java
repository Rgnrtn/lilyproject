package org.lilycms.rowlog.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.lilycms.rowlog.api.RowLogMessageListener;

public class RowLogMessageListenerMapping {

    public static RowLogMessageListenerMapping INSTANCE = new RowLogMessageListenerMapping();
    
    private Map<String, RowLogMessageListener> mapping = Collections.synchronizedMap(new HashMap<String, RowLogMessageListener>());

    private RowLogMessageListenerMapping() {
    }
    
    public void put(String id, RowLogMessageListener listenerInstance) {
        mapping.put(id, listenerInstance);
    }
    
    public RowLogMessageListener get(String id) {
        return mapping.get(id);
    }
    
    public void remove(String id) {
        mapping.remove(id);
    }
}