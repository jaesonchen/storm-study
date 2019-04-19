package com.asiainfo.storm.trident;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.trident.state.ITupleCollection;
import org.apache.storm.trident.state.OpaqueValue;
import org.apache.storm.trident.state.map.IBackingMap;

/**   
 * @Description: 内存缓存
 * 
 * @author chenzq  
 * @date 2019年4月19日 下午4:54:23
 * @version V1.0
 * @Copyright: Copyright(c) 2019 jaesonchen.com Inc. All rights reserved. 
 */
public class MemoryMapStateBacking<T> implements IBackingMap<T>, ITupleCollection {
    
    static ConcurrentHashMap<String, Map<List<Object>, Object>> _dbs = new ConcurrentHashMap<String, Map<List<Object>, Object>>();
    Map<List<Object>, T> db;
    Long currTx;

    @SuppressWarnings("unchecked")
    public MemoryMapStateBacking(String id) {
        if (!_dbs.containsKey(id)) {
            _dbs.put(id, new HashMap<>());
        }
        this.db = (Map<List<Object>, T>) _dbs.get(id);
    }
    
    public static void clearAll() {
        _dbs.clear();
    }
    
    public void multiRemove(List<List<Object>> keys) {
        for (List<Object> key: keys) {
            db.remove(key);
        }
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        List<T> list = new ArrayList<>();
        for (List<Object> key : keys) {
            list.add(db.get(key));
        }
        return list;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        for (int i = 0; i < keys.size(); i++) {
            List<Object> key = keys.get(i);
            T val = vals.get(i);
            db.put(key, val);
        }
    }
    
    @Override
    public Iterator<List<Object>> getTuples() {
        return new Iterator<List<Object>>() {

            private Iterator<Map.Entry<List<Object>, T>> it = db.entrySet().iterator();

            public boolean hasNext() {
                return it.hasNext();
            }

            public List<Object> next() {
                Map.Entry<List<Object>, T> e = it.next();
                List<Object> ret = new ArrayList<Object>();
                ret.addAll(e.getKey());
                ret.add(((OpaqueValue<?>) e.getValue()).getCurr());
                return ret;
            }

            public void remove() {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        };
    }
}
