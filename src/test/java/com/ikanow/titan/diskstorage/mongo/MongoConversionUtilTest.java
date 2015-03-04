package com.ikanow.titan.diskstorage.mongo;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;

public class MongoConversionUtilTest {

	   private static final Logger logger =
	            LoggerFactory.getLogger(MongoConversionUtilTest.class);

	   /*	@Test
	public void testStaticBuffer2MongoFieldName(){
		String badField = "$clu$ter.part\0ition.1234";
		StaticBuffer badB = new StaticArrayBuffer(badField.getBytes());
		String goodField = MongoConversionUtil.staticBuffer2MongoFieldName(badB);
		assertEquals(goodField,"@@DOLLAR@@clu@@DOLLAR@@ter@@DOT@@part@@NULL@@ition@@DOT@@1234");
		StaticBuffer convertedB = MongoConversionUtil.mongoFieldName2StaticBuffer(goodField);
		assertEquals(badB,convertedB);
		assertEquals(badB.toString(),convertedB.toString());
	}

	
	public void testStaticBuffer(){
		byte[] versionNo = new byte[]{3, 1, 48, 46, 53, 46, 48, 45, 77,177-256};
		StaticArrayBuffer ab = new StaticArrayBuffer(versionNo);
		String addValue = MongoConversionUtil.staticBuffer2String(ab);
		logger.debug("Mutate Converted StaticBuffer1 for {}={}",addValue,ab);
		StaticBuffer convertedB = MongoConversionUtil.mongoFieldValue2StaticBuffer(addValue);
		logger.debug("Mutate Converted StaticBuffer2 for {}={}",addValue,convertedB);
		assertEquals(ab,convertedB);
		assertEquals(ab.toString(),convertedB.toString());
	}
*/
	@Test
	public void testStaticBufferByteConversion(){
		byte[] versionNo = new byte[]{3, 1, 48, 46, 53, 46, 48, 45, 77,177-256};
		StaticArrayBuffer ab = new StaticArrayBuffer(versionNo);
		byte[] addValue = MongoConversionUtil.staticBuffer2Bytes(ab);
		logger.debug("Mutate Converted StaticBuffer1 for {}={}",addValue,ab);
		StaticBuffer convertedB = MongoConversionUtil.mongoFieldValue2StaticBuffer(addValue);
		logger.debug("Mutate Converted StaticBuffer2 for {}={}",addValue,convertedB);
		assertEquals(ab,convertedB);
		assertEquals(ab.toString(),convertedB.toString());
	}

	@Test
	public void testStaticBufferHexConversion(){
		byte[] versionNo = new byte[]{3, 1, 48, 46, 53, 46, 48, 45, 77,177-256};
		StaticArrayBuffer ab = new StaticArrayBuffer(versionNo);
		String addValue = MongoConversionUtil.staticBuffer2MongoFieldNameHex(ab);
		logger.debug("Mutate Converted StaticBuffer1 for {}={}",addValue,ab);
		StaticBuffer convertedB = MongoConversionUtil.mongoFieldNameHex2StaticBuffer(addValue);
		logger.debug("Mutate Converted StaticBuffer2 for {}={}",addValue,convertedB);
		assertEquals(ab,convertedB);
		assertEquals(ab.toString(),convertedB.toString());
	}
	
}
