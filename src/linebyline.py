import lzma
import os
import time


if not os.path.exists("dataset/questions.txt"):
    dest_file = "dataset/questions.txt"
    zip_file_path = os.path.abspath(f"{dest_file}.xz")
    extract_to = os.path.abspath(dest_file)
    with lzma.open(zip_file_path, "rb") as r, open(extract_to, "wb") as w:
        w.write(r.read())

start_time = time.time()

with open("dataset/questions.txt", encoding="utf-8") as f:
    u = set()
    sum = 0
    for l in f:
        for w in l.split(" "):
            sum = sum + 1
            u.add(w)

end_time = time.time()
print(f"총 출현 단어 갯수: {sum}, 고유 단어 갯수: {len(u)}, 걸린 시간: {(end_time - start_time)} 초")
