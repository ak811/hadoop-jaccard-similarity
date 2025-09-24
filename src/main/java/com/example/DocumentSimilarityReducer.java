package com.example;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.net.URI;
import java.util.*;

public class DocumentSimilarityReducer {

    // Job 1: sum |A| per document
    public static class DocSizeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable out = new IntWritable();

        @Override
        protected void reduce(Text docId, Iterable<IntWritable> ones, Context ctx) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : ones) sum += i.get();
            out.set(sum);
            ctx.write(docId, out); // "docId \t size"
        }
    }

    // Job 2 (reduce #1): from (word -> [docIds]) emit all unordered pairs once
    public static class PairIntersectionReducer extends Reducer<Text, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private final Text pairOut = new Text();

        @Override
        protected void reduce(Text word, Iterable<Text> docIds, Context ctx) throws IOException, InterruptedException {
            // gather unique docs containing this word
            List<String> docs = new ArrayList<>();
            Set<String> seen = new HashSet<>();
            for (Text d : docIds) {
                String s = d.toString();
                if (seen.add(s)) docs.add(s);
            }
            // emit each unordered pair once -> contributes +1 to intersection
            Collections.sort(docs);
            for (int i = 0; i < docs.size(); i++) {
                for (int j = i + 1; j < docs.size(); j++) {
                    pairOut.set(docs.get(i) + "," + docs.get(j));
                    ctx.write(pairOut, ONE);
                }
            }
        }
    }

    // Job 2 (reduce #2/combiner): sum intersection counts per pair
    public static class SumIntReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable out = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> vals, Context ctx) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : vals) sum += v.get();
            out.set(sum);
            ctx.write(key, out); // "docA,docB \t intersection"
        }
    }

    // Job 3: load doc sizes (cache file) and compute Jaccard for each pair
    public static class JaccardReducer extends Reducer<Text, IntWritable, Text, Text> {
        private final Map<String, Integer> docSizes = new HashMap<>();
        private final Text outVal = new Text();

        @Override
        protected void setup(Context ctx) throws IOException {
            Configuration conf = ctx.getConfiguration();
            URI[] cacheFiles = ctx.getCacheFiles();
            if (cacheFiles == null) return;
            for (URI uri : cacheFiles) {
                Path p = new Path(uri.getPath());
                FileSystem fs = FileSystem.get(uri, conf);
                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] parts = line.split("\t");
                        if (parts.length == 2) {
                            docSizes.put(parts[0].trim(), Integer.parseInt(parts[1].trim()));
                        }
                    }
                }
            }
        }

        @Override
        protected void reduce(Text pair, Iterable<IntWritable> counts, Context ctx) throws IOException, InterruptedException {
            int intersection = 0;
            for (IntWritable c : counts) intersection += c.get();

            String[] ab = pair.toString().split(",");
            if (ab.length != 2) return;
            String a = ab[0].trim();
            String b = ab[1].trim();
            Integer sizeA = docSizes.get(a);
            Integer sizeB = docSizes.get(b);
            if (sizeA == null || sizeB == null) return;

            int union = sizeA + sizeB - intersection;
            double jaccard = union > 0 ? (intersection * 1.0) / union : 0.0;
            outVal.set(String.format("Similarity: %.2f", jaccard));
            // Emit exactly as required: "<A>, <B> Similarity: <score>"
            ctx.write(new Text(a + ", " + b), outVal);
        }
    }
}
