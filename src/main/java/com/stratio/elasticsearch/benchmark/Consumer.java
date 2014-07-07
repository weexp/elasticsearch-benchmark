package com.stratio.elasticsearch.benchmark;

import java.util.List;

import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.google.common.util.concurrent.RateLimiter;

public class Consumer implements Runnable {

	private static final Logger logger = Logger.getLogger("benchmark");

	private final TransportClient client;
	private final String index;
	private final String type;
	private final int limit;
	private final boolean relevance;
	private final Stats stats;
	private final List<String> dataset;
	private final Double rate;

	public Consumer(TransportClient client,
	                String index,
	                String type,
	                int limit,
	                boolean relevance,
	                List<String> dataset,
	                Double rate,
	                Stats stats) {
		this.client = client;
		this.index = index;
		this.type = type;
		this.limit = limit;
		this.relevance = relevance;
		this.dataset = dataset;
		this.rate = rate;
		this.stats = stats;
	}

	public void run() {
		RateLimiter rateLimiter = RateLimiter.create(rate);

		for (String data : dataset) {

			rateLimiter.acquire();

			QueryBuilder qb = QueryBuilders.queryString(data);
			if (!relevance) {
				qb = QueryBuilders.constantScoreQuery(qb);
			}
			SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index)
			                                                  .setTypes(type)
			                                                  .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
			                                                  .setFrom(0)
			                                                  .setSize(limit)
			                                                  .setQuery(qb)
			                                                  .setExplain(false);

			long startTime = System.currentTimeMillis();

			SearchResponse response = searchRequestBuilder.execute().actionGet();

			long queryTime = System.currentTimeMillis() - startTime;
			stats.inc(queryTime);
			logger.debug("QUERY : " + searchRequestBuilder + " " + queryTime + " ms");

			SearchHit[] searchHits = response.getHits().getHits();
			for (SearchHit searchHit : searchHits) {
				logger.debug("\tHIT: " + searchHit.getSourceAsString());
			}
		}
	}

}
