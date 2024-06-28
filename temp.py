import json
import random

def load_and_write_data(input_file, output_file, num_entries):
    with open(input_file, 'r') as f_in:
        lines = f_in.readlines()
    random_lines = random.sample(lines, num_entries)
    with open(output_file, 'w') as f_out:
        for line in random_lines:
            f_out.write(line)

input_file = 'formatted_data.jsonl'
output_file = 'fine_tuning_data.jsonl'
num_entries = 3000
load_and_write_data(input_file, output_file, num_entries) 