package com.asiainfo.storm.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.ITupleCollection;
import org.apache.storm.trident.state.OpaqueValue;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.ValueUpdater;
import org.apache.storm.trident.state.map.MapState;
import org.apache.storm.trident.state.map.OpaqueMap;
import org.apache.storm.trident.state.map.RemovableMapState;
import org.apache.storm.trident.state.map.SnapshottableMap;
import org.apache.storm.trident.state.snapshot.Snapshottable;
import org.apache.storm.tuple.Values;

/**   
 * @Description: trident 事务状态管理
 * 
 * @author chenzq  
 * @date 2019年4月19日 下午5:06:41
 * @version V1.0
 * @Copyright: Copyright(c) 2019 jaesonchen.com Inc. All rights reserved. 
 */
public class MemoryMapState<T> implements Snapshottable<T>, ITupleCollection, MapState<T>, RemovableMapState<T> {
    
    @SuppressWarnings("rawtypes")
    MemoryMapStateBacking<OpaqueValue> _backing;
    SnapshottableMap<T> _delegate;
    List<List<Object>> _removed = new ArrayList<>();
    Long _currTx = null;

    public MemoryMapState(String id) {
        _backing = new MemoryMapStateBacking<>(id);
        _delegate = new SnapshottableMap<>(OpaqueMap.build(_backing), new Values("$MEMORY-MAP-STATE-GLOBAL$"));
    }
    
    @Override
    public T get() {
        return _delegate.get();
    }

    @Override
    public void beginCommit(Long txid) {
        _delegate.beginCommit(txid);
        if (txid == null || !txid.equals(_currTx)) {
            _backing.multiRemove(_removed);
        }
        _removed = new ArrayList<>();
        _currTx = txid;
    }

    @Override
    public void commit(Long txid) {
        _delegate.commit(txid);
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        return _delegate.multiGet(keys);
    }

    @Override
    public void multiRemove(List<List<Object>> keys) {
        List<T> nulls = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
            nulls.add(null);
        }
        // first just set the keys to null, then flag to remove them at beginning of next commit when we know the current and last value are both null
        multiPut(keys, nulls);
        _removed.addAll(keys);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater> updaters) {
        return _delegate.multiUpdate(keys, updaters);
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        _delegate.multiPut(keys, vals);
    }

    @Override
    public Iterator<List<Object>> getTuples() {
        return _backing.getTuples();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public T update(ValueUpdater updater) {
        return _delegate.update(updater);
    }

    @Override
    public void set(T o) {
        _delegate.set(o);
    }
    
    @SuppressWarnings("serial")
    public static class Factory implements StateFactory {

        String _id;

        public Factory() {
            _id = UUID.randomUUID().toString();
        }

        @SuppressWarnings("rawtypes")
        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            return new MemoryMapState<>(_id + partitionIndex);
        }
    }
}
