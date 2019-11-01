package com.thinkaurelius.titan.diskstorage.docdb;

import com.microsoft.azure.documentdb.Document;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import org.apache.commons.codec.binary.Hex;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by jmondal on 7/2/2016.
 */
public class DocDbUtils {

    public static String getHexString(StaticBuffer value) {
        ByteBuffer vBuf = value.asByteBuffer();
        byte[] vBytes = Arrays.copyOf(vBuf.array(), vBuf.limit());
        return Hex.encodeHexString(vBytes);
    }

    public static int getHashCode(StaticBuffer buffer) {
        ByteBuffer kBuf = buffer.asByteBuffer();
        byte[] kBytes = Arrays.copyOf(kBuf.array(), kBuf.limit());
        return Hex.encodeHexString(kBytes).hashCode();
    }

    public static String getTitanComparableKeyHexString(StaticBuffer key)
    {
        ByteBuffer buf = key.asByteBuffer();
        byte[] arr  = Arrays.copyOfRange(buf.array(), buf.position(), buf.limit());
        //byte[] arr = Arrays.copyOf(buf.array(), buf.limit());
        return Hex.encodeHexString(arr);
    }

    public static void print(String s) {

        //System.out.println(s);
    }

    public static Document geDocument(StaticBuffer key, StaticBuffer column, StaticBuffer value, String name) {

        int kHash = DocDbUtils.getHashCode(key);
        int colHash = DocDbUtils.getHashCode(column);

        StringBuilder sb = new StringBuilder("{");

        sb.append("\""+DocDbConstants.COMPARABLE_KEY_STRING +"\":\"").append(DocDbUtils.getTitanComparableKeyHexString(key));
        sb.append("\", \""+DocDbConstants.COMPARABLE_COLUMN_STRING +"\":\"").append(DocDbUtils.getTitanComparableKeyHexString(column));
        sb.append("\", \""+DocDbConstants.KEY_STRING +"\":\"").append(DocDbUtils.getHexString(key)).append("\", \""+DocDbConstants.KEY_OFFSET +"\":").append(key.asByteBuffer().position()).append(", \""+DocDbConstants.KEY_LIMIT +"\":").append(key.asByteBuffer().limit());
        sb.append(", \""+DocDbConstants.COLUMN_STRING +"\":\"").append(DocDbUtils.getHexString(column)).append("\", \""+DocDbConstants.COLUMN_OFFSET +"\":").append(column.asByteBuffer().position()).append(", \""+DocDbConstants.COLUMN_LIMIT +"\":").append(column.asByteBuffer().limit());
        sb.append(", \""+DocDbConstants.VALUE_STRING+"\":\"");

        // TODO NULL/UNDEFINED HANDLING
        if(value != null) {
            if (value.toString() == "") {
                sb.append("undefined").append("\", \""+DocDbConstants.VALUE_OFFSET+"\":").append("-1").append(", \""+DocDbConstants.VALUE_LIMIT+"\":").append("-1");
            } else {
                sb.append(DocDbUtils.getHexString(value)).append("\", \""+DocDbConstants.VALUE_OFFSET+"\":").append(value.asByteBuffer().position()).append(", \""+DocDbConstants.VALUE_LIMIT+"\":").append(value.asByteBuffer().limit());
            }
        }
        else
        {
            sb.append("null");
        }
        sb.append("}");

       DocDbUtils.print("Store:" + name + " Doc: "+sb.toString());

        Document doc = new Document(sb.toString());
        doc.setId(kHash+""+colHash);
        return doc;
    }
}
