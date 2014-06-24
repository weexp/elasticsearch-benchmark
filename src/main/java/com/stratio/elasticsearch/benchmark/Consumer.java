package com.stratio.elasticsearch.benchmark;

import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import com.google.common.util.concurrent.RateLimiter;

public class Consumer implements Runnable {

	private static final Logger logger = Logger.getLogger("benchmark");

	private SearchRequestBuilder searchRequestBuilder;
	private RateLimiter rateLimiter;
	private Stats stats;

	public Consumer(SearchRequestBuilder searchRequestBuilder, RateLimiter rateLimiter, Stats stats) {
		this.searchRequestBuilder = searchRequestBuilder;
		this.rateLimiter = rateLimiter;
		this.stats = stats;
	}

	public void run() {
		try {
			rateLimiter.acquire();

			long startTime = System.currentTimeMillis();

			SearchResponse response = searchRequestBuilder.execute().actionGet();

			long queryTime = System.currentTimeMillis() - startTime;
			stats.inc(queryTime);
			logger.debug("QUERY : " + searchRequestBuilder + " " + queryTime + " ms");

			SearchHits searchHits = response.getHits();
			for (SearchHit searchHit : searchHits) {
				logger.debug("\tHIT: " + searchHit.getSourceAsString());
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e);
		}
	}

}
