import os
import re

found = []
for root, dirs, files in os.walk('templates'):
    for f in files:
        if f.endswith('.html') or f.endswith('.jinja'):
            p = os.path.join(root, f)
            with open(p, 'r', encoding='utf-8') as fp:
                for idx, line in enumerate(fp, 1):
                    # Check for currency dollar
                    # 1. Jinja variable with $ prefix: ${{
                    # 2. dollar in class name: bi-currency-dollar or text 'dollar'
                    # 3. Price ($)
                    # 4. >$
                    # 5. '$' in JS concatenation for prices, e.g. '$' + 
                    # 6. ($12.00) or similar
                    has_curr = False
                    if '${{' in line:
                        has_curr = True
                    elif 'dollar' in line.lower():
                        has_curr = True
                    elif 'Price ($)' in line or 'Revenue ($)' in line:
                        has_curr = True
                    elif '>$' in line:
                        has_curr = True
                    elif "'$'" in line or '"$"' in line:
                        has_curr = True
                    elif re.search(r'\(\s*\$', line):
                        has_curr = True
                    elif re.search(r'\$\s*\d', line):
                        has_curr = True

                    if has_curr:
                        found.append((p, idx, line.strip()))

print(f"Total found: {len(found)}")
for p, idx, line in found:
    print(f"{p}:{idx}: {line}")
