package com.example;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

public class DocumentSimilarityMapper {

    // Job 1: compute |A| = number of unique words in each document
    public static class DocSizeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private final Text docIdOut = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            int firstSpace = line.indexOf(' ');
            if (firstSpace <= 0) return; // malformed
            String docId = line.substring(0, firstSpace).trim();
            String text = line.substring(firstSpace + 1);

            Set<String> unique = tokenizeToUnique(text);
            docIdOut.set(docId);
            for (String w : unique) {
                ctx.write(docIdOut, ONE);
            }
        }
    }

    // Job 2: inverted index: emit (word -> docId) once per (doc,word)
    public static class WordToDocMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text wordOut = new Text();
        private final Text docIdOut = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            int firstSpace = line.indexOf(' ');
            if (firstSpace <= 0) return; // malformed
            String docId = line.substring(0, firstSpace).trim();
            String text = line.substring(firstSpace + 1);

            Set<String> unique = tokenizeToUnique(text);
            docIdOut.set(docId);
            for (String w : unique) {
                wordOut.set(w);
                ctx.write(wordOut, docIdOut);
            }
        }
    }

    // Job 3: pass-through mapper for (pair \t intersectionCount)
    public static class PairCountPassThroughMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final Text pairOut = new Text();
        private final IntWritable countOut = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            // expect "docA,docB \t count"
            String[] parts = line.split("\t");
            if (parts.length != 2) return;
            pairOut.set(parts[0].trim());
            try {
                countOut.set(Integer.parseInt(parts[1].trim()));
                ctx.write(pairOut, countOut);
            } catch (NumberFormatException ignore) { }
        }
    }

    // --- helpers ---
    private static Set<String> tokenizeToUnique(String text) {
        // normalize: lowercase, remove punctuation -> keep letters/digits/space
        String norm = text.toLowerCase().replaceAll("[^a-z0-9\\s]", " ");
        String[] toks = norm.trim().split("\\s+");
        Set<String> unique = new LinkedHashSet<>();
        for (String t : toks) {
            if (!t.isEmpty()) unique.add(t);
        }
        return unique;
    }
}
