package com.microsoft.azure.cosmosdb.rx.patterns.onetoone;

import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.Database;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import com.microsoft.azure.cosmosdb.rx.patterns.TestConfigurations;
import com.microsoft.azure.cosmosdb.rx.patterns.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.observers.TestSubscriber;

import java.util.Date;
import java.util.Random;

import static com.microsoft.azure.cosmosdb.rx.patterns.TestUtils.HTTP_STATUS_CODE_CREATED;
import static com.microsoft.azure.cosmosdb.rx.patterns.TestUtils.HTTP_STATUS_CODE_NO_CONTENT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;


public class PlayerRepoTest {

    private static final String DATABASE_NAME = TestUtils.getDatabaseName(PlayerRepoTest.class);
    private AsyncDocumentClient client;

    @Before
    public void setUp() throws DocumentClientException {
        client = new AsyncDocumentClient.Builder()
                .withServiceEndpoint(TestConfigurations.HOST)
                .withMasterKeyOrResourceToken(TestConfigurations.MASTER_KEY)
                .withConnectionPolicy(ConnectionPolicy.GetDefault())
                .withConsistencyLevel(ConsistencyLevel.Session)
                .build();

        // Clean up before setting up
        TestUtils.cleanUpDatabase(client,DATABASE_NAME);

        var db = new Database();
        db.setId(DATABASE_NAME);
        client.createDatabase(db, null)
                .toBlocking()
                .subscribe();
    }

    @After
    public void shutdown() {
        TestUtils.safeclean(client, DATABASE_NAME);
    }

    @Test
    public void testCreateCollectionAsync() {
        var playerRepo = new PlayerRepo(client,DATABASE_NAME);

        var resourceResponse = playerRepo.CreateCollectionAsync()
                .toBlocking()
                .single();

        assertThat(resourceResponse.getStatusCode(),equalTo(HTTP_STATUS_CODE_CREATED));
    }

    @Test
    public void testAddPlayerAsync() {
        var playerRepo = new PlayerRepo(client,DATABASE_NAME);
        playerRepo.CreateCollectionAsync()
                .toBlocking()
                .subscribe();

        var player  = createPlayer("1");

        var subscriber = new TestSubscriber<Player>();
        playerRepo.AddPlayerAsync(player)
                .toBlocking()
                .subscribe(subscriber);

        subscriber.assertCompleted();
        subscriber.assertNoErrors();
        subscriber.assertValueCount(1);

    }

    @Test
    public void testGetPlayerAsync() {
        var playerRepo = new PlayerRepo(client,DATABASE_NAME);
        playerRepo.CreateCollectionAsync()
                .toBlocking()
                .subscribe();

        var expected = playerRepo.AddPlayerAsync(createPlayer("1"))
                .toBlocking()
                .single();

        var actual =  playerRepo.GetPlayerAsync(expected.getId())
                .toBlocking()
                .single();

       assertThat(actual,equalTo(expected));
    }

    @Test
    public void testUpdatePlayerAsync() {
        var playerRepo = new PlayerRepo(client,DATABASE_NAME);
        playerRepo.CreateCollectionAsync()
                .toBlocking()
                .subscribe();

        var expected = playerRepo.AddPlayerAsync(createPlayer("1"))
                .toBlocking()
                .single();

        var newName = "NewPlayerName";
        expected.setName(newName);

        var actual =  playerRepo.UpdatePlayerAsync(expected)
                .toBlocking()
                .single();

        assertThat(actual.getName(),equalTo(expected.getName()));
    }

    @Test
    public void testRemovePlayerAsync() {
        var playerRepo = new PlayerRepo(client,DATABASE_NAME);
        playerRepo.CreateCollectionAsync()
                .toBlocking()
                .subscribe();

        var player = playerRepo.AddPlayerAsync(createPlayer("1"))
                .toBlocking()
                .single();

        var resourceResponse =  playerRepo.RemovePlayerAsync(player.getId())
                .toBlocking()
                .single();

        assertThat(resourceResponse.getStatusCode(),equalTo(HTTP_STATUS_CODE_NO_CONTENT));

        var subscriber = new TestSubscriber<Player>();
        playerRepo.GetPlayerAsync(player.getId()).toBlocking().subscribe(subscriber);
        subscriber.assertError(DocumentClientException.class);
        subscriber.assertNotCompleted();

    }

    //Helpers
    private Player createPlayer(String playerId){
        var score = new Random().doubles().findFirst().getAsDouble();
        return Player.builder().id(playerId).name("Player"+playerId).score(score)
                .startTime(new Date())
                .build();
    }

}
