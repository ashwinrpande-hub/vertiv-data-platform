with open('sql/03_silver/01_silver_ddl.sql', 'r', encoding='utf-8') as f:
    content = f.read()

import re

# Fix trailing commas before closing parenthesis
content = re.sub(r',(\s*\n\s*\))', r'\1', content)

# Remove the comment line causing EOF error
content = content.replace(
    '-- SHA256 of all non-key attrs\n-- Unchanged row: same HASH_DIFF',
    '-- Change detection hash'
)

# Fix ARRAY type issue in Dynamic Tables
content = content.replace(
    'ARRAY_CONSTRUCT()',
    "PARSE_JSON('[]')"
)

with open('sql/03_silver/01_silver_ddl.sql', 'w', encoding='utf-8') as f:
    f.write(content)
print('Silver file fixed')
