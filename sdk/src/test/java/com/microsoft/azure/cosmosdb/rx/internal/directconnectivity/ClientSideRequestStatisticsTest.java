/*
 * The MIT License (MIT)
 * Copyright (c) 2018 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.microsoft.azure.cosmosdb.rx.internal.directconnectivity;

import com.microsoft.azure.cosmosdb.ConnectionMode;
import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.PartitionKey;
import com.microsoft.azure.cosmosdb.RequestOptions;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import com.microsoft.azure.cosmosdb.rx.TestConfigurations;
import com.microsoft.azure.cosmosdb.rx.TestSuiteBase;
import com.microsoft.azure.cosmosdb.rx.internal.Configs;
import com.microsoft.azure.cosmosdb.rx.internal.HttpClientFactory;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.CompositeHttpClient;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.testng.annotations.Test;

import java.net.URL;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class ClientSideRequestStatisticsTest extends TestSuiteBase {

	@Test(groups = {"emulator"}, timeOut = TIMEOUT)
	public void AddressResolutionStatistics() {
		AsyncDocumentClient client = null;
		try {
			ConnectionPolicy connectionPolicy = new ConnectionPolicy();
			connectionPolicy.setConnectionMode(ConnectionMode.Direct);
			client = new AsyncDocumentClient.Builder()
			.withConnectionPolicy(connectionPolicy)
			.withMasterKeyOrResourceToken(TestConfigurations.MASTER_KEY)
			.withServiceEndpoint(TestConfigurations.HOST)
			.build();
			Document document = new Document();
			document.setId(UUID.randomUUID().toString());
			document.set("mypk", document.getId());
			RequestOptions options = new RequestOptions();
			options.setPartitionKey(new PartitionKey(document.getId()));
			ResourceResponse<Document> writeResourceResponse = client.createDocument(SHARED_MULTI_PARTITION_COLLECTION.getSelfLink(), document, options, true)
			.toBlocking()
			.first();
			//Success address resolution client side statistics
			assertThat(writeResourceResponse.getClientSideRequestStatistics().toString()).contains("AddressResolutionStatistics");
			assertThat(writeResourceResponse.getClientSideRequestStatistics().toString()).contains("inflightRequest='false'");
			assertThat(writeResourceResponse.getClientSideRequestStatistics().toString()).doesNotContain("endTime=\"null\"");
			assertThat(writeResourceResponse.getClientSideRequestStatistics().toString()).contains("errorMessage='null'");
			assertThat(writeResourceResponse.getClientSideRequestStatistics().toString()).doesNotContain("errorMessage='java.nio.channels.ClosedChannelException");

			client = new AsyncDocumentClient.Builder()
			.withConnectionPolicy(connectionPolicy)
			.withMasterKeyOrResourceToken(TestConfigurations.MASTER_KEY)
			.withServiceEndpoint(TestConfigurations.HOST)
			.build();

			GlobalAddressResolver addressResolver = (GlobalAddressResolver) FieldUtils.readField(client, "addressResolver", true);
			Map<URL, GlobalAddressResolver.EndpointCache> addressCacheByEndpoint = (Map<URL, GlobalAddressResolver.EndpointCache>) FieldUtils.readField(addressResolver, "addressCacheByEndpoint",
			true);
			GlobalAddressResolver.EndpointCache endpointCache = (GlobalAddressResolver.EndpointCache) addressCacheByEndpoint.values().toArray()[0];

			HttpClientFactory httpClientFactory = new HttpClientFactory(new Configs());
			httpClientFactory.withRequestTimeoutInMillis(1);
			FieldUtils.writeField(httpClientFactory, "requestTimeoutInMillis", 1, true);
			CompositeHttpClient<ByteBuf, ByteBuf> httpClient = httpClientFactory.toHttpClientBuilder().build();
			FieldUtils.writeField(endpointCache.addressCache, "httpClient", httpClient, true);

			new Thread(() -> {
				try {
					Thread.sleep(5000);
					HttpClientFactory httpClientFactory1 = new HttpClientFactory(new Configs());
					CompositeHttpClient<ByteBuf, ByteBuf> httpClient1 = httpClientFactory1.toHttpClientBuilder().build();
					FieldUtils.writeField(endpointCache.addressCache, "httpClient", httpClient1, true);
				} catch (Exception e) {
					fail(e.getMessage());
				}
			}).start();
			ResourceResponse<Document> readResourceResponse = client.readDocument(writeResourceResponse.getResource().getSelfLink(), options)
			.toBlocking()
			.first();

			//Partial success address resolution client side statistics
			assertThat(readResourceResponse.getClientSideRequestStatistics().toString()).contains("AddressResolutionStatistics");
			assertThat(readResourceResponse.getClientSideRequestStatistics().toString()).contains("inflightRequest='false'");
			assertThat(readResourceResponse.getClientSideRequestStatistics().toString()).doesNotContain("endTime=\"null\"");
			assertThat(readResourceResponse.getClientSideRequestStatistics().toString()).contains("errorMessage='null'");
			assertThat(readResourceResponse.getClientSideRequestStatistics().toString()).contains("errorMessage='java.nio.channels.ClosedChannelException");
		} catch (Exception ex) {
			fail("Client initialization should not fail due to low requestTimeout on connection policy " + ex.getMessage());
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}
}
