package com.thinkaurelius.titan.diskstorage.docdb.Iterator;

import com.google.common.collect.Iterators;
import com.microsoft.azure.documentdb.Document;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.docdb.DocDbConstants;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import org.apache.commons.codec.binary.Hex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jmondal on 7/2/2016.
 */
public class DocumentKeyIterator implements KeyIterator{

    public final List<Entry> keyList;
    public int index;

    public  DocumentKeyIterator(List<Entry> keys)
    {
        this.keyList = keys;
        this.index = 0;
    }

    @Override
    public RecordIterator<Entry> getEntries() {
        return new RecordIterator<Entry>() {
            private final Iterator<Entry> entries = keyList.iterator();

            @Override
            public boolean hasNext() {
                return entries.hasNext();
            }

            @Override
            public Entry next() {
                return entries.next();
            }

            @Override
            public void close() {
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean hasNext() {
        return this.index < this.keyList.size();
    }

    @Override
    public StaticBuffer next() {
        return this.keyList.get(this.index++).getColumn();
    }
}
