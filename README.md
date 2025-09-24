# Assignment 2: Document Similarity using MapReduce

**Name:** Ali Rahimian

**Student ID:** 801371610

## Overview

This project computes **Jaccard Similarity** between all pairs of documents using **Hadoop MapReduce**.  
Given input lines of the form `DocumentID <text...>`, we tokenize and normalize each document into a **set of unique words**, then compute pairwise similarity:

$$
\mathrm{Jaccard}(A,B)=\frac{|A\cap B|}{|A\cup B|}
$$

The pipeline runs in three MapReduce jobs: (1) compute unique word counts per document, (2) build pairs via an inverted index and emit one `1` per shared word, and (3) aggregate intersections and compute Jaccard using cached doc sizes.

---

## Approach and Implementation

### Preprocessing
- Convert to **lowercase**.
- Remove **punctuation/non‑alphanumeric** chars.
- Split on whitespace.
- Treat each document as a **set** (deduplicate tokens per document).

### Job 1 — Doc Sizes (unique words per doc)
- **Mapper (DocSizeMapper)**  
  **Input:** `LongWritable, Text(line)`  
  **Output:** `(docId: Text, 1: IntWritable)` for each **unique** word in that doc.  
  (We use a set per line to ensure we emit at most one count per token per document.)
- **Reducer (DocSizeReducer)**  
  **Input:** `(docId, [1, 1, ...])`  
  **Output:** `(docId, size)` where `size = |A|` (# unique words in the document).  
  This file is later cached into Job 3.

### Job 2 — Pair Intersections (emit a “vote” per shared word)
- **Mapper (WordToDocMapper)**  
  **Input:** `LongWritable, Text(line)`  
  **Output:** `(word: Text, docId: Text)` for each **unique** word in that doc.
- **Reducer (PairIntersectionReducer)**  
  **Input:** `(word, [doc1, doc2, ...])`  
  **Output:** For each unordered pair among the doc list, emit `(pairKey: "DocA,DocB", 1)` once **per word** that both docs share.  
  > These `1`s are the “votes” from each shared word. There are many such lines in the intermediate output.

> **Important:** We **do not** set a combiner in Job 2, because the mapper value type is `Text` (docId). A combiner expecting `IntWritable` would cause a type mismatch.

### Job 3 — Jaccard (sum intersections & compute similarity)
- **Mapper (PairCountPassThroughMapper)**  
  **Input:** `(pairKey, 1)` lines from Job 2  
  **Output:** `(pairKey, 1)` (pass-through)
- **Reducer (JaccardReducer)**  
  - **Cache** loads doc sizes from Job 1 output (`part-r-00000`): a map `{docId -> size}`.  
  - Sums all `1`s for a `pairKey` to get `|A ∩ B|`.  
  - Looks up `|A|` and `|B|`, computes:
    $J=\frac{|A\cap B|}{|A|+|B|-|A\cap B|}$
  - **Output:** `("DocA, DocB", "Similarity: 0.xx")` (rounded to 2 decimals).

### Overall Data Flow
```
Input (HDFS) --> Job 1 (DocSizeMapper/Reducer)  --> /output_doc_sizes
             --> Job 2 (WordToDocMapper -> PairIntersectionReducer) --> /output_pair_counts
             --> Job 3 (PairCountPassThroughMapper -> JaccardReducer + cached /output_doc_sizes/part-r-00000) --> /output_jaccard
```

---

## Repository Layout

```
.
├── docker-compose.yml
├── hadoop.env
├── pom.xml
├── README.md  (this file)
├── src/
│   └── main/java/com/example/
│       ├── DocumentSimilarityMapper.java
│       ├── DocumentSimilarityReducer.java
│       └── controller/DocumentSimilarityDriver.java
└── datasets/
    ├── small_dataset.txt
    ├── medium_dataset.txt
    └── large_dataset.txt
```

---

## Setup and Execution (GitHub Codespaces + Docker)

### 1) Start/verify the Hadoop cluster
From the repository root:
```bash
docker compose up -d
docker compose ps
```

### 2) Build the jar
```bash
mvn -q -DskipTests clean package
```

### 3) Copy jar and datasets into the ResourceManager container
```bash
docker cp target/DocumentSimilarity-0.0.1-SNAPSHOT.jar resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
docker cp datasets/small_dataset.txt  resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
docker cp datasets/medium_dataset.txt resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
docker cp datasets/large_dataset.txt  resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

### 4) Open a shell in the container
```bash
docker exec -it resourcemanager /bin/bash
cd /opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

### 5) Put inputs into HDFS
```bash
hdfs dfs -mkdir -p /input/data
hdfs dfs -put -f small_dataset.txt  /input/data/
hdfs dfs -put -f medium_dataset.txt /input/data/
hdfs dfs -put -f large_dataset.txt  /input/data/
hdfs dfs -ls /input/data
```

### 6) Run the pipeline (3 jobs) per dataset
Use this clean pattern each time:

```bash
# CLEAN existing outputs (avoid FileAlreadyExistsException)
hdfs dfs -rm -r -f /output_doc_sizes /output_pair_counts /output_jaccard

# RUN
time hadoop jar DocumentSimilarity-0.0.1-SNAPSHOT.jar \
  com.example.controller.DocumentSimilarityDriver \
  /input/data/<DATASET>.txt \
  /output_doc_sizes \
  /output_pair_counts \
  /output_jaccard

# INSPECT
echo "--- Doc sizes ---";          hdfs dfs -cat /output_doc_sizes/part-* || true
echo "--- Pair intersections ---"; hdfs dfs -cat /output_pair_counts/part-* | head -n 40
echo "--- Jaccard ---";            hdfs dfs -cat /output_jaccard/part-*
```

### 7) Save outputs (from HDFS to container FS, then to repo)
Still inside the container:
```bash
mkdir -p /tmp/results-3dn/<dataset>
hdfs dfs -get -f /output_doc_sizes   /tmp/results-3dn/<dataset>/
hdfs dfs -get -f /output_pair_counts /tmp/results-3dn/<dataset>/
hdfs dfs -get -f /output_jaccard     /tmp/results-3dn/<dataset>/
```

Back in the **Codespaces host** terminal:
```bash
docker cp resourcemanager:/tmp/results-3dn ./results-3dn
```

> Repeat Steps 6–7 for `small_dataset.txt`, `medium_dataset.txt`, `large_dataset.txt`.

---

## Results (3 DataNodes)

These are the actual outputs I obtained and saved under `results-3dn/`.

### Small (from `results-3dn/small/output_jaccard/part-r-00000`)
```
Document1, Document2    Similarity: 0.16
Document1, Document3    Similarity: 0.15
Document2, Document3    Similarity: 0.15
```

Doc sizes (unique tokens):
```
Document1    640
Document2    641
Document3    643
```

### Medium (from `results-3dn/medium/output_jaccard/part-r-00000`)
```
Document1, Document2    Similarity: 0.17
Document1, Document3    Similarity: 0.16
Document2, Document3    Similarity: 0.16
```

Doc sizes:
```
Document1    379
Document2    382
Document3    384
```

### Large (from `results-3dn/large/output_jaccard/part-r-00000`)
```
Document1, Document2    Similarity: 0.16
Document1, Document3    Similarity: 0.15
Document2, Document3    Similarity: 0.15
```

Doc sizes:
```
Document1    640
Document2    641
Document3    643
```

---

## Challenges and Solutions

- **Type mismatch in Job 2**  
  I initially added a **combiner** expecting `IntWritable` while the Job 2 **mapper outputs `Text`** values (`docId`). Hadoop failed with:  
  `Type mismatch in value from map: expected IntWritable, received Text`.  
  **Fix:** Remove the combiner from Job 2 and set explicit `MapOutputKeyClass/ValueClass`.

- **No output with “…” input**  
  A test input like `Document1 ...` got stripped to no tokens, producing empty outputs.  
  **Fix:** Use real words; the pipeline relies on normalized tokens.

- **FileAlreadyExistsException**  
  Hadoop refuses to overwrite output directories.  
  **Fix:** Always clean outputs first:
  ```bash
  hdfs dfs -rm -r -f /output_doc_sizes /output_pair_counts /output_jaccard
  ```

- **Glued commands**  
  Accidentally pasted multiple commands on one line (e.g., `cat ... part-*hdfs dfs -rm ...`).  
  **Fix:** Put each command on its own line or separate with `&&`.

- **SASL trust warnings**  
  Messages like `SASL encryption trust check: localHostTrusted = false` are harmless in this Docker demo.

---

## How to Reproduce

1. Start the cluster (`docker compose up -d`).
2. Build the jar (`mvn -q -DskipTests clean package`).
3. `docker cp` the jar & datasets into `resourcemanager`.
4. Put inputs into HDFS.
5. Run the three-job driver for each dataset.
6. `hdfs dfs -get` outputs to `/tmp/results-*` and `docker cp` back to the repo.

---

## Sample Input (tiny demo)
```
Document1 This is a sample document containing several words here
Document2 Another document that also has several different words here too
Document3 Sample text with different tokens and words present
```

## Sample Output (tiny demo)
```
Document1, Document2    Similarity: 0.27
Document1, Document3    Similarity: 0.13
Document2, Document3    Similarity: 0.13
```

---

## Notes for Grader

- Code compiles with Maven and runs on the provided dockerized Hadoop cluster.
- Outputs for three datasets and the instructions to reproduce are included.
- Performance comparison instructions are provided; timings can be found in the commit history or filled in after running the 1‑DN tests.

