package com.stratio.elasticsearch.benchmark;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;
import org.apache.log4j.chainsaw.Main;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import com.google.common.util.concurrent.RateLimiter;

public class App {

	private static final Logger logger = Logger.getLogger("benchmark");

	public static void main(String[] args) throws IOException, InterruptedException {

		logger.info("STARTING");

		Dataset dataset = null;
		Properties properties = null;

		Options options = new Options();
		options.addOption("d", "dataset", true, "Dataset file");
		options.addOption("p", "properties", true, "Properties file");
		options.addOption("h", "help", false, "Prints help");

		try {

			// Build command line parser
			CommandLineParser parser = new BasicParser();
			CommandLine cmdLine = parser.parse(options, args);

			// Print help
			if (cmdLine.hasOption("h")) {
				new HelpFormatter().printHelp(Main.class.getCanonicalName(), options);
				System.exit(1);
			}

			if (cmdLine.hasOption("dataset")) {
				String path = cmdLine.getOptionValue("dataset");
				dataset = new Dataset(path);
			} else {
				System.out.println("Dataset path option required");
				System.exit(-1);
			}

			if (cmdLine.hasOption("properties")) {
				String path = cmdLine.getOptionValue("properties");
				InputStream is = new FileInputStream(path);
				properties = new Properties();
				properties.load(is);
				is.close();
			} else {
				System.out.println("Properties path option required");
				System.exit(-1);
			}

		} catch (org.apache.commons.cli.ParseException e) {
			new HelpFormatter().printHelp(Main.class.getCanonicalName(), options);
			System.exit(-1);
		}

		String hosts = properties.getProperty("hosts");
		String index = properties.getProperty("index");
		String type = properties.getProperty("type");
		Double rate = Double.parseDouble(properties.getProperty("rate"));
		Integer threads = Integer.parseInt(properties.getProperty("threads"));
		Integer queries = Integer.parseInt(properties.getProperty("queries"));
		Integer limit = Integer.parseInt(properties.getProperty("limit"));
		Boolean relevance = Boolean.parseBoolean(properties.getProperty("relevance"));

		Settings settings = ImmutableSettings.settingsBuilder()
		// .put("client.transport.sniff", true)
		                                     .build();
		TransportClient client = new TransportClient(settings);
		for (String host : hosts.split(",")) {
			client.addTransportAddress(new InetSocketTransportAddress(host, 9300));
		}

		RateLimiter rateLimiter = RateLimiter.create(rate);
		Stats stats = new Stats();
		ExecutorService executorService = Executors.newFixedThreadPool(threads);
		for (String data : dataset.get(queries)) {
			QueryBuilder qb = QueryBuilders.queryString(data);
			if (!relevance) {
				qb = QueryBuilders.constantScoreQuery(qb);
			}
			SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index)
			                                                  .setTypes(type)
			                                                  .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
			                                                  .setQuery(qb)
			                                                  .setSize(limit)
			                                                  .setExplain(false);
			Consumer consumer = new Consumer(searchRequestBuilder, rateLimiter, stats);
			executorService.execute(consumer);
		}
		executorService.shutdown();
		executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

		logger.info(stats.toString());

		client.close();

		System.exit(1);
	}
}
