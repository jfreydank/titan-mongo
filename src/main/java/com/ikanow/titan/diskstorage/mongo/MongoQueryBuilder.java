package com.ikanow.titan.diskstorage.mongo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRangeQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;

/** 
 * Utility class for building mongoDB objects fir querying and storing Titan's KCV data model.
 **/ 
 public class MongoQueryBuilder {
 
	 public static Set<String> RESERVED_FIELDNAMES= new HashSet<String>();
	 public static String KEY="key";
	 public static String COLUMN_NAME="c";
	 public static String COLUMN_VALUE="v";
	 public static String COLUMN_NAME_DEBUG="cd";
	 public static String VALUE_DEBUG="vd";

	 public static String INTERNAL_ID="_id";
	    static{
	    	RESERVED_FIELDNAMES.add(INTERNAL_ID);
	    	RESERVED_FIELDNAMES.add(KEY);
	 
	    }
	 
	 public static DBObject EMPTY =  new BasicDBObject();
	 
	 public static DBObject createEmptyWithKey(StaticBuffer key){
		 BasicDBObject object = new BasicDBObject();		 		 
		 byte[] entryKey = MongoConversionUtil.staticBuffer2Bytes(key);
		 //object.put(KEY, key.asByteBuffer().array());
		 //TODO check if Binary as key is more performant
		 object.put(KEY, entryKey);
		 return object;
	 }

		public static DBObject createKeyQuery(KeyRangeQuery query) {
			
			byte[] keyStart = MongoConversionUtil.staticBuffer2Bytes(query.getKeyStart());
			byte[] keyEnd = MongoConversionUtil.staticBuffer2Bytes(query.getKeyEnd());
			DBObject queryObject = QueryBuilder.start(KEY).greaterThanEquals(keyStart).and(KEY).lessThan(keyEnd).get();		
			return queryObject;
		}
		
		public static DBObject createSliceQuery(SliceQuery query) {
				String columnNameStart = MongoConversionUtil.staticBuffer2MongoFieldNameHex(query.getSliceStart());
				String columnNameEnd = MongoConversionUtil.staticBuffer2MongoFieldNameHex(query.getSliceEnd());
				DBObject queryObject = QueryBuilder.start(COLUMN_NAME).greaterThanEquals(columnNameStart).and(COLUMN_NAME).lessThan(columnNameEnd).get();		
				return queryObject;
		}

		public static DBObject createKeyColumnQuery(StaticBuffer keyBuf,StaticBuffer columnNameBuf) {
			String keyHex = MongoConversionUtil.staticBuffer2MongoFieldNameHex(keyBuf);
			String columnNameHex = MongoConversionUtil.staticBuffer2MongoFieldNameHex(columnNameBuf);
			DBObject queryObject = QueryBuilder.start(KEY).is(keyHex).and(COLUMN_NAME).is(columnNameHex).get();		
			return queryObject;
		}
		
		 public static DBObject createNew(StaticBuffer keyBuf,StaticBuffer columnNameBuf,StaticBuffer value){
			 BasicDBObject object = new BasicDBObject();		 		 
			String keyHex = MongoConversionUtil.staticBuffer2MongoFieldNameHex(keyBuf);
			String columnNameHex = MongoConversionUtil.staticBuffer2MongoFieldNameHex(columnNameBuf);
			String valueHex = MongoConversionUtil.staticBuffer2MongoFieldNameHex(value);
			object.put(KEY, keyHex);
			object.put(COLUMN_NAME, columnNameHex);
			object.put(COLUMN_VALUE, valueHex);
			 return object;
		 }

			public static DBObject createKeySliceQuery(KeySliceQuery query) {
				String key = MongoConversionUtil.staticBuffer2MongoFieldNameHex(query.getKey());
				String columnNameStart = MongoConversionUtil.staticBuffer2MongoFieldNameHex(query.getSliceStart());
				String columnNameEnd = MongoConversionUtil.staticBuffer2MongoFieldNameHex(query.getSliceEnd());
				DBObject queryObject = QueryBuilder.start(KEY).is(key).and(COLUMN_NAME).greaterThanEquals(columnNameStart).and(COLUMN_NAME).lessThan(columnNameEnd).get();		
				return queryObject;
			}
			
			public static DBObject createKeyRangeQuery(KeyRangeQuery query) {
				String keyStart = MongoConversionUtil.staticBuffer2MongoFieldNameHex(query.getKeyStart());
				String keyEnd = MongoConversionUtil.staticBuffer2MongoFieldNameHex(query.getKeyEnd());
				String columnNameStart = MongoConversionUtil.staticBuffer2MongoFieldNameHex(query.getSliceStart());
				String columnNameEnd = MongoConversionUtil.staticBuffer2MongoFieldNameHex(query.getSliceEnd());
				DBObject queryObject = QueryBuilder.start(KEY).greaterThanEquals(keyStart).and(KEY).lessThan(keyEnd).and(COLUMN_NAME).greaterThanEquals(columnNameStart).and(COLUMN_NAME).lessThan(columnNameEnd).get();		
				return queryObject;
			}

		    public static Entry convertMongoObjectToEntry(DBObject mongoEntry) {
		    	Entry entry = null;
		    	if(mongoEntry!=null){    		
					//StaticBuffer sbKey = MongoConversionUtil.mongoFieldNameHex2StaticBuffer((String)mongoEntry.get(KEY));
					StaticBuffer sbColumn = MongoConversionUtil.mongoFieldNameHex2StaticBuffer((String)mongoEntry.get(COLUMN_NAME));
					StaticBuffer sbValue = MongoConversionUtil.mongoFieldNameHex2StaticBuffer((String)mongoEntry.get(COLUMN_VALUE));
					entry = StaticArrayEntry.of(sbColumn, sbValue);
		    	}
				return entry;
			}
		    
		    public static DBObject createKeySliceQuery(String key, SliceQuery query) {
				//String key = MongoConversionUtil.staticBuffer2MongoFieldNameHex(query.getKey());
				String columnNameStart = MongoConversionUtil.staticBuffer2MongoFieldNameHex(query.getSliceStart());
				String columnNameEnd = MongoConversionUtil.staticBuffer2MongoFieldNameHex(query.getSliceEnd());
				DBObject queryObject = QueryBuilder.start(KEY).is(key).and(COLUMN_NAME).greaterThanEquals(columnNameStart).and(COLUMN_NAME).lessThan(columnNameEnd).get();		
				return queryObject;
			}

			public static DBObject createKeysSliceQuery(List<StaticBuffer> keys, SliceQuery query) {
				
				String columnNameStart = MongoConversionUtil.staticBuffer2MongoFieldNameHex(query.getSliceStart());
				String columnNameEnd = MongoConversionUtil.staticBuffer2MongoFieldNameHex(query.getSliceEnd());
				ArrayList<String> hexKeys = new ArrayList<String>();
				for (Iterator<StaticBuffer> it = keys.iterator(); it.hasNext();) {					
					String key = MongoConversionUtil.staticBuffer2MongoFieldNameHex((StaticBuffer) it.next());
					hexKeys.add(key);
				}
				DBObject queryObject = QueryBuilder.start(KEY).in(hexKeys).and(COLUMN_NAME).greaterThanEquals(columnNameStart).and(COLUMN_NAME).lessThan(columnNameEnd).get();		
				return queryObject; 				
			}

		    public static EntryList convertMongoCursorToEntryList(DBCursor dbCursor) {
		    	EntryList entryList = new MemoryEntryList(1);
		    	try {
		            while(dbCursor.hasNext()) {
		                DBObject mongoEntry = dbCursor.next();
		                Entry entry = MongoQueryBuilder.convertMongoObjectToEntry(mongoEntry);
		                entryList.add(entry);
		            } // while
		            
		         } finally {
		         	dbCursor.close();
		         }
		    	return entryList;
		    }

			public static Map<StaticBuffer, EntryList> convertMongoCursorToEntryMap(DBCursor dbCursor) {
				Map<StaticBuffer,EntryList> resultMap = Maps.newHashMap();
				try {
		            while(dbCursor.hasNext()) {
		                DBObject mongoEntry = dbCursor.next();
		                Entry entry = MongoQueryBuilder.convertMongoObjectToEntry(mongoEntry);
						StaticBuffer sbKey = MongoConversionUtil.mongoFieldNameHex2StaticBuffer((String)mongoEntry.get(KEY));
		                EntryList entryList = resultMap.get(sbKey);		                
		                if(entryList == null){
		                	entryList = new MemoryEntryList(1);
		                	resultMap.put(sbKey, entryList);
		                }
		                
		                entryList.add(entry);
		            } // while
		            
		         } finally {
		         	dbCursor.close();
		         }				
				return resultMap;
			}
		    
}
