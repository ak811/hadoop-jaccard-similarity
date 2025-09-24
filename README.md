
# Assignment 2 — Document Similarity using MapReduce

**Name:** Ali Rahimian  
**Student ID:** 801371610

---

## Overview

This project computes **Jaccard Similarity** between all pairs of documents using **Hadoop MapReduce**.  
Given input lines of the form `DocumentID <text...>`, we tokenize and normalize each document into a **set of unique words**, then compute pairwise similarity:

$$
\mathrm{Jaccard}(A,B)=\frac{|A\cap B|}{|A\cup B|}
$$

The pipeline runs in three MapReduce jobs:  
1. Compute unique word counts per document.  
2. Build pairs via an inverted index and emit one `1` per shared word.  
3. Aggregate intersections and compute Jaccard using cached doc sizes.

---

## Approach and Implementation

### Preprocessing
- Convert to **lowercase**.
- Strip **punctuation** / non‑alphanumerics.
- Split on whitespace.
- Treat each document as a **set** (deduplicate tokens per document).

### Job 1 — Doc Sizes (unique words per doc)
- **Mapper (DocSizeMapper):** emits `(docId, 1)` for each unique word in that doc.  
- **Reducer (DocSizeReducer):** sums to `(docId, size)` = number of unique tokens.

### Job 2 — Pair Intersections (shared word “votes”)
- **Mapper (WordToDocMapper):** `(word, docId)` for each unique word in that doc.  
- **Reducer (PairIntersectionReducer):** for each `(word, [doc_i])`, emit one line `(pairKey, 1)` once per shared word.

### Job 3 — Jaccard (final computation)
- **Mapper (PairCountPassThroughMapper):** passes `(pairKey, 1)`.  
- **Reducer (JaccardReducer):**  
  - Loads cached doc sizes.  
  - Sums the intersections.  
  - Computes $J=\frac{|A\cap B|}{|A|+|B|-|A\cap B|}$.  
  - Outputs similarity rounded to 2 decimals.

---

## Why Tokens Look Like `a1`, `a15`, `common6`, …

- `a*`, `b*`, `c*` tokens are mostly **document-specific**.  
- `common*` tokens are **shared** across docs.  
- Numbers are IDs only, not semantic.

This makes it easy to control overlap and expected Jaccard values.

---

## Setup and Execution

### Steps (Docker + Hadoop)
```bash
docker compose up -d
mvn -q -DskipTests clean package
docker cp target/DocumentSimilarity-0.0.1-SNAPSHOT.jar resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
docker cp datasets/*.txt resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
hdfs dfs -mkdir -p /input/data
hdfs dfs -put -f datasets/*txt /input/data/
hdfs dfs -rm -r -f /output_doc_sizes /output_pair_counts /output_jaccard

time hadoop jar DocumentSimilarity-0.0.1-SNAPSHOT.jar   com.example.controller.DocumentSimilarityDriver   /input/data/<DATASET>.txt   /output_doc_sizes /output_pair_counts /output_jaccard
```

Inspect results with:
```bash
hdfs dfs -cat /output_doc_sizes/part-*
hdfs dfs -cat /output_pair_counts/part-* | head -n 40
hdfs dfs -cat /output_jaccard/part-*
```

---

## Input Samples

### Tiny Example (toy)
```
Document1 This is a sample document containing several words here
Document2 Another document that also has several different words here too
Document3 Sample text with different tokens and words present
```

### Small Dataset (excerpt)
```
Document1 a1 a15 common6 a57 a37 a1 common14 a1 a45 ...
Document2 b29 b46 b20 b53 b17 b15 b36 b24 common31 ...
Document3 c56 common10 c3 c17 c27 c28 c13 c7 common7 ...
```

### Medium Dataset (excerpt)
```
Document1 common115 a13 a135 a296 a26 a87 common87 ...
Document2 common121 b11 b18 b21 b245 common56 ...
Document3 common135 c19 c27 c155 c88 common73 ...
```

### Large Dataset (excerpt)
```
Document1 a276 common23 a159 a387 a11 common138 a123 ...
Document2 b199 common23 b311 b402 b18 b221 common138 ...
Document3 c77 c142 common23 common138 c155 c189 ...
```

---

## Output Samples

### Tiny Example (toy)
```
--- Doc sizes ---
Document1    9
Document2    10
Document3    8

--- Jaccard ---
Document1, Document2    Similarity: 0.27
Document1, Document3    Similarity: 0.13
Document2, Document3    Similarity: 0.13
```

### Small Dataset (3-DataNode run)
```
--- Doc sizes ---
Document1    640
Document2    641
Document3    643

--- Jaccard ---
Document1, Document2    Similarity: 0.16
Document1, Document3    Similarity: 0.15
Document2, Document3    Similarity: 0.15
```

### Medium Dataset (3-DataNode run)
```
--- Doc sizes ---
Document1    379
Document2    382
Document3    384

--- Jaccard ---
Document1, Document2    Similarity: 0.17
Document1, Document3    Similarity: 0.16
Document2, Document3    Similarity: 0.16
```

### Large Dataset (3-DataNode run)
```
--- Doc sizes ---
Document1    640
Document2    641
Document3    643

--- Jaccard ---
Document1, Document2    Similarity: 0.16
Document1, Document3    Similarity: 0.15
Document2, Document3    Similarity: 0.15
```

**Note:** The **Small** and **Large** datasets yield identical doc sizes because they share the same **unique vocabularies** per document. The large dataset has more lines/repetitions, but repetitions do not increase unique word counts.

---

## Repository Layout
```
.
├── docker-compose.yml
├── hadoop.env
├── pom.xml
├── README.md
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

## Key Notes

- Jaccard near **1.00** ⇒ nearly identical vocabularies.  
- Jaccard near **0.15–0.17** ⇒ moderate overlap with many unique tokens.  
- Identical doc sizes across datasets arise when the unique vocabularies don’t change.  
- To create differences, add **new distinct tokens** in the larger dataset.  
