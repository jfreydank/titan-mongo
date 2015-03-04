package com.ikanow.titan.diskstorage.mongo;

import org.junit.Test;

import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;



public class MongoKeyColumnValueStoreTest extends KeyColumnValueStoreTest {

	@Override
	public KeyColumnValueStoreManager openStorageManager()
			throws StorageException {		
		MongoStoreManager s = new MongoStoreManager(MongoStorageSetup.getMongoConfiguration());
		return s;
	}

/*	 @Test
	 public void insertingGettingAndDeletingSimpleDataWorks() throws Exception {
		 super.insertingGettingAndDeletingSimpleDataWorks();		 
	 }
*/
	 @Test
	 public void selectedTests() throws Exception {
		 /*super.intervalTest1();		 
		 super.deleteColumnsTest1();		 
		 super.deleteColumnsTest2();
		 super.getNonExistentKeyReturnsNull();
		 super.storeAndRetrievePerformance();
		 
		 super.storeAndRetrieve();
		 super.containsKeyReturnsFalseOnNonexistentKey();
		 super.testGetKeysWithSliceQuery();
		 */
		 super.testGetSlices();
		/*
		  super.deleteKeys();
		 */
		
	 }
}
