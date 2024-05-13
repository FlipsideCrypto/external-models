while true; do
    # Execute the query and capture the output
    q_result=$(dbt run-operation execute_and_print_query)

    # Use grep and sed to extract numbers, assuming they represent tag identifiers
    numbers=($(echo "$q_result" | grep '<agate.Row:' | sed "s/.*<agate.Row: ('\(.*\)')>.*/\1/"))

    # Format the extracted numbers as dbt tag selectors
    tag_selectors=$(printf "tag:%s " "${numbers[@]}")
    tag_selectors=$(echo "$tag_selectors" | sed 's/ $//') # Remove trailing space
    echo $tag_selectors

    # Check if tag_selectors is not empty and matches the pattern
    if [[ -n "$tag_selectors" && "$tag_selectors" =~ ^tag:[0-9]+ ]]; then
        echo "Valid tags received: $tag_selectors"
        dbt run --select "$tag_selectors"
        
        # Run the dependent models
        dbt run -m models/defillama/silver/silver__defillama_historical_yields.sql models/defillama/bronze/bronze__defillama_historical_backfill_list.sql
    else
        echo "No valid tags to run, exiting loop"
        break
    fi
done
