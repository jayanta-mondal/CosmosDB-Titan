package com.thinkaurelius.titan.diskstorage.docdb;

import java.util.Map;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

/**
 * Created by jmondal on 6/29/2016.
 */
public class DocDBStoreTransaction extends AbstractStoreTransaction {

    public static DocDBStoreTransaction getTx(StoreTransaction txh) {
        Preconditions.checkArgument(txh != null);
        Preconditions
                .checkArgument(txh instanceof DocDBStoreTransaction, "Unexpected transaction type %s", txh.getClass().getName());
        return (DocDBStoreTransaction) txh;
    }

    /**
     * This is only used for toString for debugging purposes.
     */
    private final String id;
    private final Map<StaticBuffer, Map<StaticBuffer, StaticBuffer>> expectedValues = Maps.newHashMap();
    private AbstractDocDbStore store;

    public DocDBStoreTransaction(BaseTransactionConfig config) {
        super(config);
        id = "CONSTANT_HEX" + Long.toHexString(System.nanoTime());
    }

    public String getId() {
        return id;
    }

    @Override
    public void commit() throws BackendException {
        releaseLocks();
        expectedValues.clear();
        super.commit();
    }

    private void releaseLocks() {
        for(final Map.Entry<StaticBuffer, Map<StaticBuffer, StaticBuffer>> entry : expectedValues.entrySet()) {
            final StaticBuffer key = entry.getKey();
            for(final StaticBuffer column : entry.getValue().keySet()) {
                // ToDo
                //store.releaseLock(key, column);
            }
        }
    }

    public boolean contains(StaticBuffer key, StaticBuffer column) {
        if (expectedValues.containsKey(key)) {
            return expectedValues.get(key).containsKey(column);
        }
        return false;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof DocDBStoreTransaction)) {
            return false;
        }
        DocDBStoreTransaction rhs = (DocDBStoreTransaction) obj;
        return new EqualsBuilder()
                .append(id, rhs.id)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    public StaticBuffer get(StaticBuffer key, StaticBuffer column) {
        // This method assumes the caller has called contains(..) and received a positive response
        return expectedValues.get(key)
                .get(column);
    }

    public void put(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue) {
        Map<StaticBuffer, StaticBuffer> valueMap;
        if (expectedValues.containsKey(key)) {
            valueMap = expectedValues.get(key);
        } else {
            valueMap = Maps.newHashMap();
            expectedValues.put(key, valueMap);
        }

        // Ignore any calls to put if we already have an expected value
        if (!valueMap.containsKey(column)) {
            valueMap.put(column, expectedValue);
        }
    }

    @Override
    public void rollback() throws BackendException {
        releaseLocks();
        expectedValues.clear();
        super.rollback();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append(id).append(expectedValues).toString();
    }

    public void setStore(AbstractDocDbStore abstractDocDBStore) {
        this.store = abstractDocDBStore;
    }
}
