package com.microsoft.azure.documentdb.rx.internal;

import static org.assertj.core.api.Assertions.assertThat;

import com.microsoft.azure.documentdb.Attachment;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.FeedResponsePage;
import com.microsoft.azure.documentdb.Offer;
import com.microsoft.azure.documentdb.PartitionKeyRange;
import com.microsoft.azure.documentdb.Permission;
import com.microsoft.azure.documentdb.Resource;
import com.microsoft.azure.documentdb.ResourceResponse;
import com.microsoft.azure.documentdb.StoredProcedure;
import com.microsoft.azure.documentdb.Trigger;
import com.microsoft.azure.documentdb.User;
import com.microsoft.azure.documentdb.UserDefinedFunction;
import com.microsoft.azure.documentdb.rx.AsyncDocumentClient;

import rx.Subscriber;

public class ReadFeedInternal {
    public static class CountSubscriber<T extends Resource> {
        public int count;
        public int target;
        
        public Subscriber<FeedResponsePage<T>> subscriber;

        public CountSubscriber(boolean isInitialRead) {

            subscriber = new Subscriber<FeedResponsePage<T>>() {

                @Override
                public void onStart() {
                    count = 0;
                }

                @Override
                public void onCompleted() {
                    if (!isInitialRead) {
                        assertThat(count).isEqualTo(target);
                    }
                }

                @Override
                public void onError(Throwable e) {
                    e.printStackTrace();
                }

                @Override
                public void onNext(FeedResponsePage<T> page) {
                    count += page.getResults().size();
                }
            };
        }
    }

    public static class CrudSubscriber<T extends Resource> {
        public Subscriber<ResourceResponse<T>> subscriber;
        public int target;
        
        public CrudSubscriber(AsyncDocumentClient client, FeedOptions feedOptions, String className, String link) {

            subscriber = new Subscriber<ResourceResponse<T>>() {

                @Override
                public void onCompleted() {
                }

                @Override
                public void onError(Throwable e) {
                    e.printStackTrace();
                }

                @Override
                public void onNext(ResourceResponse<T> resourceResponse) {
                    
                    if (className.equals("Database")) {
                        
                        CountSubscriber<Database> databaseCountSubscriber = new CountSubscriber<Database>(false);
                        databaseCountSubscriber.target = target;
                        client.readDatabases(feedOptions).toBlocking().subscribe(databaseCountSubscriber.subscriber);
                        
                    } else if (className.equals("DocumentCollection")){
                        
                        CountSubscriber<DocumentCollection> collectionCountSubscriber = new CountSubscriber<DocumentCollection>(false);
                        collectionCountSubscriber.target = target;
                        client.readCollections(link, feedOptions).toBlocking().subscribe(collectionCountSubscriber.subscriber);
                        
                    } else if (className.equals("Document")){
                        
                        CountSubscriber<Document> documentCountSubscriber = new CountSubscriber<Document>(false);
                        documentCountSubscriber.target = target;
                        client.readDocuments(link, feedOptions).toBlocking().subscribe(documentCountSubscriber.subscriber);
                        
                    } else if (className.equals("Trigger")){
                        
                        CountSubscriber<Trigger> triggerCountSubscriber = new CountSubscriber<Trigger>(false);
                        triggerCountSubscriber.target = target;
                        client.readTriggers(link, feedOptions).toBlocking().subscribe(triggerCountSubscriber.subscriber);
                        
                    } else if (className.equals("User")){
                        
                        CountSubscriber<User> userCountSubscriber = new CountSubscriber<User>(false);
                        userCountSubscriber.target = target;
                        client.readUsers(link, feedOptions).toBlocking().subscribe(userCountSubscriber.subscriber);
                        
                    } else if (className.equals("StoredProcedure")){
                        
                        CountSubscriber<StoredProcedure> sprocCountSubscriber = new CountSubscriber<StoredProcedure>(false);
                        sprocCountSubscriber.target = target;
                        client.readStoredProcedures(link, feedOptions).toBlocking().subscribe(sprocCountSubscriber.subscriber);
                        
                    } else if (className.equals("UserDefinedFunction")){
                        
                        CountSubscriber<UserDefinedFunction> udfCountSubscriber = new CountSubscriber<UserDefinedFunction>(false);
                        udfCountSubscriber.target = target;
                        client.readUserDefinedFunctions(link, feedOptions).toBlocking().subscribe(udfCountSubscriber.subscriber);
                        
                    } else if (className.equals("Permission")){
                        
                        CountSubscriber<Permission> permissionCountSubscriber = new CountSubscriber<Permission>(false);
                        permissionCountSubscriber.target = target;
                        client.readPermissions(link, feedOptions).toBlocking().subscribe(permissionCountSubscriber.subscriber);
                        
                    } else if (className.equals("Attachment")){
                        
                        CountSubscriber<Attachment> attachmentCountSubscriber = new CountSubscriber<Attachment>(false);
                        attachmentCountSubscriber.target = target;
                        client.readAttachments(link, feedOptions).toBlocking().subscribe(attachmentCountSubscriber.subscriber);
                        
                    } else if (className.equals("Offer")){
                        
                        CountSubscriber<Offer> offerCountSubscriber = new CountSubscriber<Offer>(false);
                        offerCountSubscriber.target = target;
                        client.readOffers(feedOptions).toBlocking().subscribe(offerCountSubscriber.subscriber);
                        
                    } else if (className.equals("PartitionKeyRange")){
                        
                        CountSubscriber<PartitionKeyRange> pkrCountSubscriber = new CountSubscriber<PartitionKeyRange>(false);
                        pkrCountSubscriber.target = target;
                        client.readPartitionKeyRanges(link, feedOptions).toBlocking().subscribe(pkrCountSubscriber.subscriber);
                    }
                }
            };
        }
    }
}
