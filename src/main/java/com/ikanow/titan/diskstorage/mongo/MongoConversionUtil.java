package com.ikanow.titan.diskstorage.mongo;

import java.nio.charset.Charset;
import java.util.Set;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;

public class MongoConversionUtil {
    private static final Logger logger = LoggerFactory.getLogger(MongoConversionUtil.class);

    
	
/*	public static String staticBuffer2MongoFieldName(StaticBuffer b) {
		String ms = new String(b.as(StaticBuffer.ARRAY_FACTORY),Charset.forName("UTF-8"));
		// replace . in field name with
		ms = ms.replaceAll("\\.","@@DOT@@");
		ms = ms.replaceAll("\\$","@@DOLLAR@@");
		ms = ms.replaceAll("\\u0000","@@NULL@@");
        return ms;
    }

	public static StaticBuffer mongoFieldName2StaticBuffer(String fieldName) {		
		String s = fieldName.replaceAll("@@DOT@@","\\.");
		s = s.replaceAll("@@DOLLAR@@","\\$");
		s = s.replaceAll("@@NULL@@","\0");
		StaticArrayBuffer staticBuffer = new StaticArrayBuffer(s.getBytes(Charset.forName("UTF-8")));
        return staticBuffer;
    }
	*/
	public static String staticBuffer2String(final StaticBuffer b) {
		return new String(b.as(StaticBuffer.ARRAY_FACTORY),Charset.forName("UTF-8"));
    }

	public static byte[] staticBuffer2Bytes(final StaticBuffer b) {
		/*int length = b.length();
		byte[] bb1 = new byte[length];
		for (int i = 0; i < length; i++) {
			bb1[i]=b.getByte(i);
		}
		*/
		byte[] asArraybb  = b.as(StaticBuffer.ARRAY_FACTORY);
		//logger.debug("staticBuffer2Bytes :{} {} {}",b,bb1,asArraybb);
		return  asArraybb;
    }

/*	public static StaticBuffer mongoFieldValue2StaticBuffer(String value) {	
		byte[] bb = value.getBytes();
		StaticArrayBuffer staticBuffer = new StaticArrayBuffer(bb);
        return staticBuffer;
    }
*/
	public static StaticBuffer mongoFieldValue2StaticBuffer(byte[] value) {	
		StaticArrayBuffer staticBuffer = new StaticArrayBuffer(value);		
        return staticBuffer;
    }

	public static String staticBuffer2MongoFieldNameHex(StaticBuffer sb) {
		byte[] data = sb.as(StaticBuffer.ARRAY_FACTORY);
		String hexString = Hex.encodeHexString(data);
        return hexString;
    }

	public static StaticBuffer mongoFieldNameHex2StaticBuffer(String data) {	
		byte[] bb = null;
		try {
			bb = Hex.decodeHex(data.toCharArray());
			StaticArrayBuffer staticBuffer = new StaticArrayBuffer(bb);
	        return staticBuffer;
		} catch (DecoderException e) {			
			logger.error("Error decoding :"+data,e);
		}
		// TODO check if null values are valid
		return null;
    }
	

}
