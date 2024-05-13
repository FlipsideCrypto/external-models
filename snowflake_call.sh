# Execute the query and capture the output
q_result=$(dbt run-operation execute_and_print_query)

# Use grep and sed to extract numbers, assuming they represent tag identifiers
numbers=($(echo "$q_result" | grep '<agate.Row:' | sed "s/.*<agate.Row: ('\(.*\)')>.*/\1/"))

# Format the extracted numbers as dbt tag selectors
tag_selectors=$(printf "tag:%s " "${numbers[@]}")

# Trim any trailing whitespace and output
echo "$tag_selectors" | sed 's/ $//'