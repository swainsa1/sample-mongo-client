import java.util.*;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import org.bson.Document;
import java.util.Arrays;
import com.mongodb.Block;

import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import java.util.List;

public class MongoClientTest {

    static MongoClient createMongoClient(){

        return new MongoClient(new MongoClientURI("mongodb://swainsa1-cosmos:qEWz024C9yAtKbiliMhuK6KwLgS9E0t9goOhGocRxgJ7nxw23GcZXANbsgHNWzAo2GfWPff5owAaoeaHy4ozLQ==@swainsa1-cosmos.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@swainsa1-cosmos@&retrywrites=false"));

    }


    public static void main (String [] args){

        long milisec =System.currentTimeMillis();
        MongoDatabase database = createMongoClient().getDatabase("mymongodb");

        MongoCollection<Document> collection = database.getCollection("collection-"+milisec);

        testInsertDoc(collection);
        testInsertDocList(collection);

        countDocuments(collection);
        query_first(collection);
        query_filter_1(collection);
        query_filter_block(collection);

        printCollection(collection);

        updateSingleDocument(collection);
        updateMultipleDocuments(collection);

        printCollection(collection);
    }

    static void testInsertDoc(MongoCollection<Document> collection){
        Document doc = new Document("name", "MongoDB")
                .append("type", "database")
                .append("count", 1)
                .append("versions", Arrays.asList("v3.2", "v3.0", "v2.6"))
                .append("info", new Document("x", 203).append("y", 102));
        collection.insertOne(doc);
    }

    static void testInsertDocList(MongoCollection collection){
        List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < 1000; i++) {
           Document d1 = new Document("account_id", i+100000).append("account_name","Name").append("Location","location1");

            documents.add(d1);
        }
        collection.insertMany(documents);

    }

    static void updateSingleDocument(MongoCollection<Document> collection){
        collection.updateOne(eq("account_id", 14), new Document("$set", new Document("i", 500000)));
    }

    static void updateMultipleDocuments(MongoCollection<Document> collection){
        UpdateResult updateResult = collection.updateMany(lt("account_id", 10), inc("account_id", 1000));
        System.out.println(updateResult.getModifiedCount());
    }

    static void countDocuments(MongoCollection<Document> collection){
        System.out.println("Documents in collection: " + collection.count());
    }

    static void query_first(MongoCollection<Document> collection){
        Document myDoc = collection.find().first();
        System.out.println(myDoc.toJson());
    }

    static void query_filter_1(MongoCollection<Document> collection){
        Document myDoc = collection.find(eq("account_id", 100001)).first();
        System.out.println(myDoc.toJson());
    }

    static void query_filter_block(MongoCollection<Document> collection){
        Block<Document> printBlock = new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document.toJson());
            }
        };
        collection.find(gt("account_id", 100001)).forEach(printBlock);
    }

    static void printCollection(MongoCollection<Document> collection){
        MongoCursor<Document> cursor = collection.find().iterator();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }

    }

}
