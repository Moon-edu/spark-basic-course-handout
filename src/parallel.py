import time
from concurrent import futures
import os
import splitter

def get_partial_count(num: int):
    print(f"DEBUG: Process {num} start")
    
    FILE=f"dataset/split/questions-{num}.txt"
    word_cnt=set()
    sum = 0

    with open(FILE, "r", encoding="utf-8") as f:
        for n in f:
            for w in n.split(" "):
                word_cnt.add(w)
                sum = sum + 1
    print(f"DEBUG: Process {num} done")
    return (sum, word_cnt)

def run():
    NUM_FILE = 8
    if not os.path.exists("dataset/split") or len(os.listdir("dataset/split")) != NUM_FILE:
        splitter.run(NUM_FILE)

    start_time = time.time()

    inputs = list(range(1, NUM_FILE + 1))
    with futures.ProcessPoolExecutor(max_workers=NUM_FILE) as executor:
        results = list(executor.map(get_partial_count, inputs)) # Collection of partial count

    print("DEBUG: Aggregate result")
    sum = 0
    all_word_cnt = set() # Aggregate to single dictionary
    for partial_sum, word_set in results:
        sum = sum + partial_sum
        all_word_cnt = all_word_cnt.union(word_set)

    end_time = time.time()
    print(f"총 출현 단어 갯수: {sum}, 고유 단어 갯수: {len(all_word_cnt)}, 걸린 시간: {(end_time - start_time)} 초")

if __name__ == "__main__":
    run()