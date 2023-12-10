import os
import math
import lzma
from pathlib import Path




def run(NUM_FILE=8):
    INPUT_FILE = "dataset/questions.txt"
    OUTPUT_PATH = "dataset/split"
    OUTPUT_PATTERN = OUTPUT_PATH + "/questions-{NUM}.txt"
    TOTAL_LINE = 5264573

    if not os.path.exists("dataset/questions.txt"):
        dest_file = "dataset/questions.txt"
        zip_file_path = os.path.abspath(f"{dest_file}.xz")
        extract_to = os.path.abspath(dest_file)
        with lzma.open(zip_file_path, "rb") as r, open(extract_to, "wb") as w:
            w.write(r.read())

    splitted_root_dir = os.path.abspath("dataset/split")
    print(f"DEBUG: splitted root = {splitted_root_dir}")
    Path(splitted_root_dir).mkdir(parents=True, exist_ok=True)

    if len(os.listdir(splitted_root_dir)) == NUM_FILE:
        print("DEBUG: Files are already splitted")
        exit(0)

    elif len(os.listdir(splitted_root_dir)) > 0:
        for p in os.listdir(splitted_root_dir):
            to_delete = os.path.join(splitted_root_dir, p)
            print(f"DEBUG: Delete {to_delete}")
            os.remove(to_delete)


    line_per_file = math.ceil(TOTAL_LINE / NUM_FILE)
    print(f"DEBUG: Max line number of each file is {line_per_file}")

    cur_file_num = 1
    cur_num_line = 0
    file_to_write = OUTPUT_PATTERN.replace("{NUM}", str(cur_file_num))
    output = open(file_to_write, 'w', encoding="utf-8")
    print(f"DEBUG: Writing to {file_to_write}")

    with open(INPUT_FILE, 'r', encoding="utf-8") as f:
        for l in f:
            cur_num_line = cur_num_line + 1
            output.write(l)
            if cur_num_line >= line_per_file:
                cur_file_num = cur_file_num + 1
                output.close()
                file_to_write = OUTPUT_PATTERN.replace("{NUM}", str(cur_file_num))
                print(f"DEBUG: Writing to {file_to_write}")
                output = open(file_to_write, 'w', encoding='utf-8')
                cur_num_line = 0

    if not output.closed:
        output.close()

if __name__ == "__main__":
    run()
