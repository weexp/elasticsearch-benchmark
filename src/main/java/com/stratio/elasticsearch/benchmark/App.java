package com.stratio.elasticsearch.benchmark;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
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
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

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
		                                     .put("client.transport.sniff", false)
		                                     .put("client.transport.nodes_sampler_interval", 60)
		                                     .put("cluster.name", "wikipedia-benchmark")
		                                     .build();
		TransportClient client = new TransportClient(settings);
		for (String host : hosts.split(",")) {
			System.out.println("ADDING CLIENT " + host);
			client.addTransportAddress(new InetSocketTransportAddress(host, 9300));
		}
		client.connectedNodes();

		Stats stats = new Stats();
		ExecutorService executorService = Executors.newFixedThreadPool(threads);
		for (int i = 0; i < threads; i++) {
			List<String> data = dataset.get(queries);
			Consumer consumer = new Consumer(client, index, type, limit, relevance, data, rate, stats);
			executorService.execute(consumer);
		}
		executorService.shutdown();
		executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

		logger.info(stats.toString());

		client.close();

		System.exit(1);
	}
}
