import os
import re

files_to_check = []
for root, dirs, files in os.walk('.'):
    if '.venv' in root or '.git' in root or '__pycache__' in root or 'scratch' in root:
        continue
    for f in files:
        if f.endswith(('.html', '.py')):
            files_to_check.append(os.path.join(root, f))

# Look for currency patterns
currency_dollar_regex = re.compile(r'(\$\{\{|\bPrice\s*\(\$\)|\bRevenue\s*\(\$\)|bi-currency-dollar|\$\s*[\'"]|[\'"]\s*\$\s*[\'"]|\$\s*\d|Order\s*\$\{\{|\$\s*<\span|>\s*\$\s*<|\bDollar|\bdollar)')

matches_by_file = {}
for fp in files_to_check:
    with open(fp, 'r', encoding='utf-8', errors='ignore') as file:
        for idx, line in enumerate(file, 1):
            if currency_dollar_regex.search(line):
                # Exclude mongo query operators like '$set', '$inc', '$in', etc.
                if re.search(r'[\'"]\$(set|inc|in|nin|ne|gt|gte|lt|lte|match|group|push|sum|toDouble|round)[\'"]', line):
                    continue
                matches_by_file.setdefault(fp, []).append((idx, line.strip()))

for fp, match_list in matches_by_file.items():
    print(f"\nFILE: {fp} ({len(match_list)} matches)")
    for l_no, text in match_list:
        print(f"  [{l_no}]: {text}")
