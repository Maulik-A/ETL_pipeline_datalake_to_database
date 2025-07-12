S3_BUCKET = 'mybucket'
S3_PREFIXES = ['AB', 'CD', 'EF']  # Example prefixes for each CSV type
DB_URI = 'postgresql://localhost:5432/postgres'
EXPECTED_SCHEMAS = {
    "CD": ["Production Code", "Unit ID", "Column A Stage A", "Column B Stage A", "Column C Stage A", "Column D Stage A" ],
    "AB": ["Production Code", "Parent ID", "Child Position", "Operator", "Column A Stage A", "Column B Stage A" ],
    "EF": ["Production Code", "Parent ID", "Child Position", "Comment", "Column A Stage A", "Column B Stage A", "Column C Stage A"]
}