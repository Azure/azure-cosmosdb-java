package com.microsoft.azure.cosmosdb.rx.patterns.manytomany;

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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

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
    public void testCreatePlayerLookupCollectionAsync(){
        var gameRepo = new GameRepo(client,DATABASE_NAME);

        var resourceResponse = gameRepo.CreatePlayerLookupCollectionAsync()
                .toBlocking()
                .single();

        assertThat(resourceResponse.getStatusCode(),equalTo(HTTP_STATUS_CODE_CREATED));
    }

    @Test
    public void testAddGameAsync(){
        var gameRepo = new GameRepo(client,DATABASE_NAME);
        gameRepo.CreatePlayerLookupCollectionAsync()
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
    public void testGetGamesAsync() {
        var gameRepo = new GameRepo(client,DATABASE_NAME);
        gameRepo.CreatePlayerLookupCollectionAsync()
                .toBlocking()
                .subscribe();

        var expectedList = addGames("1", 10);

        var actualList =  gameRepo.GetGameByPlayerIdAsync("1")
                .toList()
                .toBlocking()
                .single();

        assertThat(actualList, containsInAnyOrder(expectedList.toArray()));

    }

    @Test
    public void testGetGameInfrequentAsync() {
        var gameRepo = new GameRepo(client,DATABASE_NAME);
        gameRepo.CreatePlayerLookupCollectionAsync()
                .toBlocking()
                .subscribe();

        var expectedList = List.of(

                addGames("1", 10)
                .stream()
                .filter((game) -> game.getGameId().equals("1"))
                .findFirst()
                .get(),

                addGames("2", 10)
                .stream()
                .filter((game) -> game.getGameId().equals("1"))
                .findFirst()
                .get()
        );


        var actualList =  gameRepo.GetGamesInfrequentAsync("1")
                .toList()
                .toBlocking()
                .single();

        assertThat(actualList, containsInAnyOrder(expectedList.toArray()));
    }

    @Test
    public void testCreateGameLookupCollectionAsync() {
        var gameRepo = new GameRepo(client,DATABASE_NAME);

        var resourceResponse = gameRepo.CreateGameLookupCollectionAsync()
                .toBlocking()
                .single();

        assertThat(resourceResponse.getStatusCode(),equalTo(HTTP_STATUS_CODE_CREATED));
    }


    @Test
    public void testSyncUpAndGetGameAsync() throws Exception {
        var gameRepo = new GameRepo(client,DATABASE_NAME);

        //create source collection
        gameRepo.CreatePlayerLookupCollectionAsync()
                .toBlocking()
                .single();

        //create destination collection
        gameRepo.CreateGameLookupCollectionAsync()
                .toBlocking()
                .single();

        //addGames creates and adds documents to source playerLookupCollection
        addGames("1", 10);
        addGames("2", 10);

        var syncSubscription = gameRepo.SyncGameLookupCollectionAsync();

        // Sleep to give enough time for syncup
        Thread.sleep(2000);

        var actualList =  gameRepo.GetGamesByGameIdAsync("1")
                .toList()
                .toBlocking()
                .single();

        assertThat(actualList, hasSize(2));

        addGames("3", 10);

        // Sleep to give enough time for syncup
        Thread.sleep(2000);

        actualList =  gameRepo.GetGamesByGameIdAsync("1")
                .toList()
                .toBlocking()
                .single();

        assertThat(actualList, hasSize(3));

        syncSubscription.unsubscribe();
    }


    // Helpers
    private Game createGame(int gameId, String playerId){
        var sGameId = String.valueOf(gameId);
        var score = new Random().doubles().findFirst().getAsDouble();
        return Game.builder().gameId(sGameId).playerId(playerId).score(score)
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
