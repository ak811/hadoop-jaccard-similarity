# Assignment 2 — Document Similarity using MapReduce

**Name:** Ali Rahimian  
**Student ID:** 801371610

---

## Overview

This project computes **Jaccard Similarity** between all pairs of documents using **Hadoop MapReduce**.  
Given input lines of the form `DocumentID <text...>`, we tokenize and normalize each document into a **set of unique words**, then compute pairwise similarity:

$$\mathrm{Jaccard}(A,B)=\frac{|A\cap B|}{|A\cup B|}$$

The pipeline runs in three MapReduce jobs:
1. **Doc Sizes:** compute unique word counts per document.  
2. **Pair Intersections:** build pairs via an inverted index and emit one `1` per shared word.  
3. **Jaccard:** aggregate intersections and compute final similarity using cached doc sizes.

Outputs follow the required format:
```
<DocumentID1>, <DocumentID2> Similarity: <score>
```

---

## Approach and Implementation

### Preprocessing
- Convert to **lowercase**.
- Strip **punctuation/non‑alphanumerics**.
- Split on whitespace.
- Treat each document as a **set** (deduplicate tokens within each document).

### Job 1 — Doc Sizes (unique words per doc)
- **Mapper (`DocSizeMapper`)**: emits `(docId, 1)` per unique token in the document.  
- **Reducer (`DocSizeReducer`)**: sums counts to produce `(docId, size)`.

### Job 2 — Pair Intersections (shared word “votes”)
- **Mapper (`WordToDocMapper`)**: emits `(word, docId)` for each unique token in a document.  
- **Reducer (`PairIntersectionReducer`)**: for each `word`, creates all unique doc pairs appearing with that word and emits `(pairKey, 1)`.

### Job 3 — Jaccard (final computation)
- **Mapper (`PairCountPassThroughMapper`)**: pass-through of `(pairKey, 1)`.  
- **Reducer (`JaccardReducer`)**:
  - Loads cached doc sizes from Job 1.
  - Sums intersections for each pair to get `|A ∩ B|`.
  - Computes $J=\frac{|A\cap B|}{|A|+|B|-|A\cap B|}$ and rounds to **two decimals**.
  - Emits `"A, B Similarity: 0.XX"`.

---

## Datasets

I generated three clearly distinct datasets with increasing total tokens and per-document sizes to make timing differences tangible.

### Generator (`datagen.py`)
```python
# datagen.py
# Creates larger datasets:
#   datasets/small.txt  ≈ 10k tokens total (50 docs × 200 tokens)
#   datasets/medium.txt ≈ 40k tokens total (100 docs × 400 tokens)
#   datasets/large.txt  ≈ 120k tokens total (150 docs × 800 tokens)

import os, random, math
RNG = random.Random(6190)

def make_vocab(n):
    width = len(str(n))
    return [f"w{str(i).zfill(width)}" for i in range(1, n + 1)]

def split_topics(vocab, k=8):
    chunk = math.ceil(len(vocab)/k)
    return [vocab[i*chunk:(i+1)*chunk] for i in range(k)]

def gen_doc(tokens_per_doc, topics, topic_share=0.7):
    t = RNG.randrange(len(topics))
    topic_vocab = topics[t]
    global_vocab = [w for tt in topics for w in tt]
    n_topic = int(tokens_per_doc*topic_share)
    n_noise = tokens_per_doc - n_topic
    toks = [RNG.choice(topic_vocab) for _ in range(n_topic)]
    toks += [RNG.choice(global_vocab) for _ in range(n_noise)]
    RNG.shuffle(toks)
    return toks

def write_dataset(path, num_docs, tokens_per_doc, vocab_size):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    vocab  = make_vocab(vocab_size)
    topics = split_topics(vocab, k=8)
    with open(path, "w", encoding="utf-8") as f:
        for i in range(1, num_docs+1):
            toks = gen_doc(tokens_per_doc, topics, topic_share=0.7)
            f.write(f"Document{i} " + " ".join(toks) + "\\n")
    total_tokens = num_docs * tokens_per_doc
    print(f"[OK] {path}  docs={num_docs}, tokens/doc={tokens_per_doc}, "
          f"~total_tokens={total_tokens}, vocab_size={vocab_size}")

def main():
    small  = (50,  200,  500)   # (docs, tokens/doc, vocab_size)
    medium = (100, 400, 1500)
    large  = (150, 800, 3000)
    write_dataset("datasets/small.txt",  *small)
    write_dataset("datasets/medium.txt", *medium)
    write_dataset("datasets/large.txt",  *large)

if __name__ == "__main__":
    main()
```

### Actual dataset sizes (from `wc`)
```
50  10050   50541 datasets/small.txt
100 40100  241092 datasets/medium.txt
150 120150 721692 datasets/large.txt
```

---

## Build

```bash
mvn -DskipTests clean package
# jar: target/DocumentSimilarity-0.0.1-SNAPSHOT.jar
# main class: com.example.controller.DocumentSimilarityDriver
```

---

## Running on Hadoop (Codespaces + Docker)

### Option A — **3 DataNodes** (default compose)

Bring the cluster up and copy the artifacts:
```bash
docker compose up -d

# copy jar + datasets into ResourceManager
docker cp target/DocumentSimilarity-0.0.1-SNAPSHOT.jar \
  resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/app.jar
docker exec resourcemanager bash -lc 'mkdir -p /opt/hadoop-3.2.1/share/hadoop/mapreduce/datasets'
docker cp datasets/. resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/datasets/
```

Run each dataset (repeat for `small|medium|large`):
```bash
docker exec resourcemanager bash -lc '
  HADOOP=/opt/hadoop-3.2.1/bin/hadoop
  HDFS=/opt/hadoop-3.2.1/bin/hdfs
  JAR=/opt/hadoop-3.2.1/share/hadoop/mapreduce/app.jar
  DS=/opt/hadoop-3.2.1/share/hadoop/mapreduce/datasets/medium.txt  # change per dataset

  "$HDFS" dfs -rm -r -f /input/medium /output_doc_sizes /output_pair_counts /output_jaccard
  "$HDFS" dfs -mkdir -p /input/medium
  "$HDFS" dfs -put -f "$DS" /input/medium/

  set -o pipefail
  /bin/bash -lc "time -p \"$HADOOP\" jar \"$JAR\" com.example.controller.DocumentSimilarityDriver \
      /input/medium/medium.txt /output_doc_sizes /output_pair_counts /output_jaccard" \
      2>&1 | tee /tmp/run_medium_3dn.log

  "$HDFS" dfs -cat /output_jaccard/part-* > /tmp/jaccard_medium_3dn.txt
'

# copy results back to the repo
mkdir -p results-3dn/medium/output_jaccard
docker cp resourcemanager:/tmp/run_medium_3dn.log results-3dn/medium/timing.txt
docker cp resourcemanager:/tmp/jaccard_medium_3dn.txt results-3dn/medium/output_jaccard/part-r-00000
```

### Option B — **1 DataNode** (alternate compose file)

A separate compose file `docker-compose-single-datanode.yml` is included. Use it like this:
```bash
docker compose -f docker-compose-single-datanode.yml up -d

# verify one live DN
docker exec resourcemanager bash -lc '/opt/hadoop-3.2.1/bin/hdfs dfsadmin -report | grep -E "Live datanodes|Datanodes available" -A2'
# should show: Live datanodes (1)
```

Run each dataset with the same absolute-path commands as above, saving under `results-1dn/...`.

---

## Results

All outputs are committed for reproducibility:

- **3 DataNodes**  
  - `results-3dn/small|medium|large/output_jaccard/part-r-00000`  
  - `results-3dn/small|medium|large/timing.txt`

- **1 DataNode**  
  - `results-1dn/small|medium|large/output_jaccard/part-r-00000`  
  - `results-1dn/small|medium|large/timing.txt`

### Timing Comparison

The exact timings captured via `time -p` (`real`) are:

| Dataset | 3 DataNodes (real s) | 1 DataNode (real s) | Speedup (3DN/1DN) |
|---|---:|---:|---:|
| small | 56.68 | 5.97 | 9.49× |
| medium | 53.67 | 8.00 | 6.71× |
| large | 57.90 | 10.09 | 5.74× |

#### Observations
- In this GitHub Codespaces setup (single VM running multiple Docker containers), the **1‑DataNode runs were faster**.  
  Multiple HDFS/YARN containers compete for the same CPU, memory, and disk, while distributed overhead
  (shuffle, scheduling, RPC) grows with more containers.
- **Correctness is identical** between 1‑DN and 3‑DN runs (Jaccard outputs match); only wall‑clock time differs.
- Larger datasets increase pair generation and shuffle volume; on a real multi‑machine cluster with independent hosts, 3+ DataNodes typically outperform 1.

---

## Example Output Snippets

```
Document1, Document10   Similarity: 0.09
Document1, Document11   Similarity: 0.10
Document1, Document12   Similarity: 0.07
Document1, Document13   Similarity: 0.10
Document1, Document14   Similarity: 0.11
```

---

## Challenges & Solutions

- **PATH/Hadoop CLI not found inside containers**  
  *Solution:* call Hadoop tools by **absolute path** (`/opt/hadoop-3.2.1/bin/hadoop` and `/opt/hadoop-3.2.1/bin/hdfs`).

- **Switching to 1 DataNode left extra DNs running**  
  *Solution:* use a separate compose file (`docker-compose-single-datanode.yml`) and relaunch with  
  `docker compose -f docker-compose-single-datanode.yml down --remove-orphans` then `up -d`.

- **Dataset differences not reflected in unique counts**  
  *Solution:* generate datasets with **larger per‑doc sizes** and **new vocabulary** so both runtime and set sizes change meaningfully.

---

## Repository Layout

```
.
├── docker-compose.yml                          # 3-DataNode cluster
├── docker-compose-single-datanode.yml          # 1-DataNode cluster
├── pom.xml
├── README.md
├── datagen.py
├── compare_timings.py
├── src/main/java/com/example/
│   ├── ... mappers / reducers ...
│   └── controller/DocumentSimilarityDriver.java
├── datasets/
│   ├── small.txt
│   ├── medium.txt
│   └── large.txt
├── results-3dn/
│   ├── small|medium|large/timing.txt
│   └── small|medium|large/output_jaccard/part-r-00000
└── results-1dn/
    ├── small|medium|large/timing.txt
    └── small|medium|large/output_jaccard/part-r-00000
```

