package com.stratio.elasticsearch.benchmark;

public class Stats {

	private long numQueries = 0;
	private long totalQueryTime = 0;
	private long executionStartTime = System.currentTimeMillis();

	public Stats() {
	}

	public synchronized void inc(long queryTime) {
		numQueries++;
		totalQueryTime += queryTime;
	}

	@Override
	public String toString() {
		long executionTime = System.currentTimeMillis() - executionStartTime;
		StringBuilder builder = new StringBuilder();
		builder.append("Stats [numQueries=");
		builder.append(numQueries);
		builder.append(", totalQueryTime=");
		builder.append(totalQueryTime);
		builder.append(", averageQueryTime=");
		builder.append(totalQueryTime / numQueries);
		builder.append(", executionTime=");
		builder.append(executionTime);
		builder.append(", throughput=");
		builder.append(numQueries * 1000 / executionTime);
		builder.append("]");
		return builder.toString();
	}

}
