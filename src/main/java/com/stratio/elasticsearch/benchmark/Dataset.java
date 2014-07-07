package com.stratio.elasticsearch.benchmark;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class Dataset implements Closeable, Iterable<String> {

	private List<String> lines;

	public Dataset(String path) throws IOException {
		lines = new LinkedList<>();
		File file = new File(path);
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line;
		while ((line = br.readLine()) != null) {
			lines.add(line);
		}
		br.close();
	}

	public Iterator<String> iterator() {
		return lines.iterator();
	}

	public List<String> get(int n) {
		Integer counter = new Integer(n);
		List<String> result = new ArrayList<>(n);
		while (counter > 0) {
			if (counter < lines.size()) {
				result.addAll(lines.subList(0, counter));
			} else {
				result.addAll(lines);
			}
			counter = n - result.size();
		}
		Collections.shuffle(result); // Randomize for mitigate caching
		return result;
	}

	public void close() throws IOException {
	}

}
