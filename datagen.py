# datagen.py
# Generates three datasets with distinct per-document sizes and total sizes:
#   datasets/small.txt  (~1k tokens total; ~50 tokens/doc)
#   datasets/medium.txt (~3k tokens total; ~100 tokens/doc)
#   datasets/large.txt  (~5k tokens total; ~125 tokens/doc)
#
# Each line: "DocumentX" followed by tokens (lowercase, no punctuation).
# Reproducible via fixed random seed.

import argparse, os, random, math
from collections import Counter

RNG = random.Random(42)

def make_vocab(n):
    # w0001, w0002, ...
    width = len(str(n))
    return [f"w{str(i).zfill(width)}" for i in range(1, n + 1)]

def pick_topic_vocab(vocab, k_topics=5):
    # Split vocab into k roughly-equal topical bins for overlap structure
    chunk = math.ceil(len(vocab) / k_topics)
    topics = []
    for i in range(k_topics):
        topics.append(vocab[i*chunk:(i+1)*chunk])
    return topics

def gen_doc(tokens_per_doc, topics, topic_share=0.6):
    # Choose a topic; draw ~60% tokens from that topic, the rest global noise
    topic_idx = RNG.randrange(len(topics))
    topic_vocab = topics[topic_idx]
    global_vocab = [w for t in topics for w in t]
    n_topic = int(tokens_per_doc * topic_share)
    n_noise = tokens_per_doc - n_topic
    toks = [RNG.choice(topic_vocab) for _ in range(n_topic)]
    toks += [RNG.choice(global_vocab) for _ in range(n_noise)]
    RNG.shuffle(toks)
    return toks

def write_dataset(path, num_docs, tokens_per_doc, vocab_size):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    vocab = make_vocab(vocab_size)
    topics = pick_topic_vocab(vocab, k_topics=5)

    with open(path, "w", encoding="utf-8") as f:
        for i in range(1, num_docs + 1):
            toks = gen_doc(tokens_per_doc, topics, topic_share=0.6)
            f.write(f"Document{i} " + " ".join(toks) + "\n")

    # quick metrics
    total_tokens = 0
    total_unique = set()
    per_doc_unique = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split()
            doc_tokens = parts[1:]
            total_tokens += len(doc_tokens)
            total_unique.update(doc_tokens)
            per_doc_unique.append(len(set(doc_tokens)))
    avg_doc_unique = sum(per_doc_unique)/len(per_doc_unique)

    print(f"[OK] {path}")
    print(f"  docs={num_docs}, tokens_per_doc≈{tokens_per_doc}, vocab_size={vocab_size}")
    print(f"  total_tokens={total_tokens}, corpus_unique={len(total_unique)}, "
          f"avg_doc_unique≈{avg_doc_unique:.1f}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="datasets", help="output directory")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    global RNG
    RNG = random.Random(args.seed)

    small  = (20,  50,  200)   # docs, tokens/doc, vocab_size
    medium = (30, 100,  600)
    large  = (40, 125, 1000)

    write_dataset(os.path.join(args.outdir, "small.txt"),  *small)
    write_dataset(os.path.join(args.outdir, "medium.txt"), *medium)
    write_dataset(os.path.join(args.outdir, "large.txt"),  *large)

if __name__ == "__main__":
    main()
