package cn.ce.oplog.mongo.net;

import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;

public class Temp {

	public static void main(String[] args) {

		try {

			Mongo mongo = new Mongo("localhost", 27017);

			DB db = mongo.getDB("local");
			// Set<String> collections = db.getCollectionNames();
			// for (String collectionName : collections) {
			// System.out.println(collectionName);
			// }
			DBCollection collection = db.getCollection("oplog.$main");
			BasicDBObject search = new BasicDBObject();
			search.append("op", new BasicDBObject("$ne", "n"));
//			search.append("ts.$ts", new BasicDBObject("$gt", 1284479136));
//			search.append(key, val)
			DBCursor cursor = collection.find(search);

			while (cursor.hasNext()) {
				System.out.println(System.currentTimeMillis());
				System.out.println(cursor.next());
			}

			System.out.println("The Search Query has Executed!");

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
