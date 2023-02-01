{% docs __overview__ %}

# Welcome to the Flipside Crypto External Models Documentation!

## **What does this documentation cover?**
The documentation included here details the design of the External tables and views available via [Flipside Crypto.](https://flipsidecrypto.xyz/earn). For more information on how these models are built, please see [the github repository.](https://github.com/FlipsideCrypto/external-models)

### **Quick Links to Table Documentation**

**Token Flow: Ethereum**
[Token Flow Documentation](https://docs.tokenflow.live/)

- [tokenflow_eth__blocks](https://flipsidecrypto.github.io/external-models/#!/source/source.external_models.tokenflow_eth.blocks)
- [tokenflow_eth__calls](https://flipsidecrypto.github.io/external-models/#!/source/source.external_models.tokenflow_eth.calls)
- [tokenflow_eth__events](https://flipsidecrypto.github.io/external-models/#!/source/source.external_models.tokenflow_eth.events)
- [tokenflow_eth__state_diffs](https://flipsidecrypto.github.io/external-models/#!/source/source.external_models.tokenflow_eth.state_diffs)
- [tokenflow_eth__storage_diffs](https://flipsidecrypto.github.io/external-models/#!/source/source.external_models.tokenflow_eth.storage_diffs)
- [tokenflow_eth__storage_reads](https://flipsidecrypto.github.io/external-models/#!/source/source.external_models.tokenflow_eth.storage_reads)
- [tokenflow_eth__transactions](https://flipsidecrypto.github.io/external-models/#!/source/source.external_models.tokenflow_eth.transactions)

**Token Flow: Starknet**
[Token Flow Documentation](https://docs.tokenflow.live/)

- [tokenflow_starknet__decoded_blocks](https://flipsidecrypto.github.io/external-models/#!/source/source.external_models.tokenflow_starknet_decoded.blocks)
- [tokenflow_starknet__decoded_events](https://flipsidecrypto.github.io/external-models/#!/source/source.external_models.tokenflow_starknet_decoded.events)
- [tokenflow_starknet__decoded_messages](https://flipsidecrypto.github.io/external-models/#!/source/source.external_models.tokenflow_starknet_decoded.messages)
- [tokenflow_starknet__decoded_traces](https://flipsidecrypto.github.io/external-models/#!/source/source.external_models.tokenflow_starknet_decoded.traces)
- [tokenflow_starknet__decoded_transactions](https://flipsidecrypto.github.io/external-models/#!/source/source.external_models.tokenflow_starknet_decoded.transactions)
- [tokenflow_starknet__l1_data_blocks](https://flipsidecrypto.github.io/external-models/#!/source/source.external_models.tokenflow_starknet_l1_data.blocks)
- [tokenflow_starknet__l1_data_contracts](https://flipsidecrypto.github.io/external-models/#!/source/source.external_models.tokenflow_starknet_l1_data.contracts)
- [tokenflow_starknet__l1_data_messages](https://flipsidecrypto.github.io/external-models/#!/source/source.external_models.tokenflow_starknet_l1_data.messages)
- [tokenflow_starknet__l1_data_storage_diffs](https://flipsidecrypto.github.io/external-models/#!/source/source.external_models.tokenflow_starknet_l1_data.storage_diffs)


## **Data Model Overview**

`EXTERNAL` is our blockchain-agonistic database for datasets derived from independently managed, external sources. While these models are built a few different ways, the primary method used is through calling internal functions that leverage curated datasets, such as Token Flow, to create accessible sql models for the analytics community. These models follow our standard approach, built using three layers of sql models: **bronze, silver, and gold (or core).** However, when the models are built externally (non-Flipside), the naming conventions may vary and models will be placed into schemas based on the source.

- Bronze: Data is loaded in from the source as a view
- Silver: All necessary parsing, filtering, de-duping, and other transformations are done here
- Gold (or core): Final views and tables that are available in Velocity

The dimension tables are sourced from a variety of on-chain and off-chain sources.

Convenience views (denoted ez_) are a combination of different fact and dimension tables. 

A user-defined-function (UDF) is available to decode hex encoded values to integers in this database. You can call this UDF by using `external.public.udf_hex_to_int(FIELD::string)`.

## **Using dbt docs**
### Navigation

You can use the ```Project``` and ```Database``` navigation tabs on the left side of the window to explore the models in the project.

### Database Tab

This view shows relations (tables and views) grouped into database schemas. Note that ephemeral models are *not* shown in this interface, as they do not exist in the database.

### Graph Exploration

You can click the blue icon on the bottom-right corner of the page to view the lineage graph of your models.

On model pages, you'll see the immediate parents and children of the model you're exploring. By clicking the Expand button at the top-right of this lineage pane, you'll be able to see all of the models that are used to build, or are built from, the model you're exploring.

Once expanded, you'll be able to use the ```--models``` and ```--exclude``` model selection syntax to filter the models in the graph. For more information on model selection, check out the [dbt docs](https://docs.getdbt.com/docs/model-selection-syntax).

Note that you can also right-click on models to interactively filter and explore the graph.


### **More information**
- [Flipside](https://flipsidecrypto.xyz/earn)
- [Velocity](https://app.flipsidecrypto.com/velocity?nav=Discover)
- [Tutorials](https://docs.flipsidecrypto.com/our-data/tutorials)
- [Github](https://github.com/FlipsideCrypto/external-models)
- [Query Editor Shortcuts](https://docs.flipsidecrypto.com/velocity/query-editor-shortcuts)
- [What is dbt?](https://docs.getdbt.com/docs/introduction)



{% enddocs %}