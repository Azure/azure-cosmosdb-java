package com.microsoft.azure.cosmosdb.rx.patterns.onetomany;

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
import rx.Observable;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;

import java.util.Date;
import java.util.List;
import java.util.Random;

import static com.microsoft.azure.cosmosdb.rx.patterns.TestUtils.HTTP_STATUS_CODE_CREATED;
import static com.microsoft.azure.cosmosdb.rx.patterns.TestUtils.HTTP_STATUS_CODE_NO_CONTENT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class GameRepoTest {

    private static final String DATABASE_NAME = TestUtils.getDatabaseName(GameRepoTest.class);
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
        var gameRepo = new GameRepo(client,DATABASE_NAME);

        var resourceResponse = gameRepo.CreateCollectionAsync()
                .toBlocking()
                .single();

        assertThat(resourceResponse.getStatusCode(),equalTo(HTTP_STATUS_CODE_CREATED));
    }

    @Test
    public void testAddGameAsync() {
        var gameRepo = new GameRepo(client,DATABASE_NAME);
        gameRepo.CreateCollectionAsync()
                .toBlocking()
                .subscribe();

        var game  = createGame(1,"1");

        var subscriber = new TestSubscriber<Game>();
        gameRepo.AddGameAsync(game)
                .toBlocking()
                .subscribe(subscriber);

        subscriber.assertCompleted();
        subscriber.assertNoErrors();
        subscriber.assertValueCount(1);
    }

    @Test
    public void testGetGameAsync() {
        var gameRepo = new GameRepo(client,DATABASE_NAME);
        gameRepo.CreateCollectionAsync()
                .toBlocking()
                .subscribe();

        var expected = gameRepo.AddGameAsync(createGame(1,"1"))
                .toBlocking()
                .single();

        var actual =  gameRepo.GetGameAsync(expected.getPlayerId(),expected.getId())
                .toBlocking()
                .single();

        assertThat(actual,equalTo(expected));
    }

    @Test
    public void testGetGamesAsync() {
        var gameRepo = new GameRepo(client,DATABASE_NAME);
        gameRepo.CreateCollectionAsync()
                .toBlocking()
                .subscribe();

        var expectedList = addGames("1",10);

        var actualList =  gameRepo.GetGamesAsync("1")
                .toList()
                .toBlocking()
                .single();

        assertThat(actualList, containsInAnyOrder(expectedList.toArray()));

    }

    @Test
    public void testUpdateGameAsync() {
        var gameRepo = new GameRepo(client,DATABASE_NAME);
        gameRepo.CreateCollectionAsync()
                .toBlocking()
                .subscribe();

        var expected = gameRepo.AddGameAsync(createGame(1,"1"))
                .toBlocking()
                .single();

        var newScore = (double) 200;
        expected.setScore(newScore);

        var actual =  gameRepo.UpdateGameAsync(expected)
                .toBlocking()
                .single();

        assertThat(actual.getScore(),equalTo(expected.getScore()));
    }

    @Test
    public void testRemoveGameAsync() {
        var gameRepo = new GameRepo(client,DATABASE_NAME);
        gameRepo.CreateCollectionAsync()
                .toBlocking()
                .subscribe();

        var game = gameRepo.AddGameAsync(createGame(1,"1"))
                .toBlocking()
                .single();

        var resourceResponse =  gameRepo.RemoveGameAsync(game.getPlayerId(),game.getId())
                .toBlocking()
                .single();

        assertThat(resourceResponse.getStatusCode(),equalTo(HTTP_STATUS_CODE_NO_CONTENT));

        var subscriber = new TestSubscriber<Game>();
        gameRepo.GetGameAsync(game.getPlayerId(),game.getId()).toBlocking().subscribe(subscriber);
        subscriber.assertError(DocumentClientException.class);
        subscriber.assertNotCompleted();

    }

    //Helpers
    private Game createGame(int gameId, String playerId){
        var sGameId = String.valueOf(gameId);
        var score = new Random().doubles().findFirst().getAsDouble();

        return Game.builder().id(sGameId).playerId(playerId).score(score)
                .startTime(new Date())
                .build();
    }

    private List<Game> addGames(String playerId, int numberOfGames) {
        var gameRepo = new GameRepo(client,DATABASE_NAME);
        // Subscribe to multiple AddGame Observables in parallel
        return Observable
                .range(1,numberOfGames)
                .map((i) -> createGame(i,playerId))
                .flatMap(game ->
                        gameRepo
                                .AddGameAsync(game)
                                .subscribeOn(Schedulers.io()))
                .toList()
                .toBlocking()
                .single();
    }

}
