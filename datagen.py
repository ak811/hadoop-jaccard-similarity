# datagen.py
# Creates larger, clearly distinct datasets:
#   datasets/small.txt  ≈ 10k tokens total   (50 docs × 200 tokens)
#   datasets/medium.txt ≈ 40k tokens total   (100 docs × 400 tokens)
#   datasets/large.txt  ≈ 120k tokens total  (150 docs × 800 tokens)
#
# Each line: "DocumentX <tokens...>" (lowercase, no punctuation).
# Deterministic via seed; documents share topical overlap so Jaccard isn't trivial.

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
            f.write(f"Document{i} " + " ".join(toks) + "\n")

    # quick summary
    total_tokens = num_docs * tokens_per_doc
    print(f"[OK] {path}  docs={num_docs}, tokens/doc={tokens_per_doc}, "
          f"~total_tokens={total_tokens}, vocab_size={vocab_size}")

def main():
    # (docs, tokens/doc, vocab_size)
    small  = (50,  200,  500)
    medium = (100, 400, 1500)
    large  = (150, 800, 3000)

    write_dataset("datasets/small.txt",  *small)
    write_dataset("datasets/medium.txt", *medium)
    write_dataset("datasets/large.txt",  *large)

if __name__ == "__main__":
    main()
