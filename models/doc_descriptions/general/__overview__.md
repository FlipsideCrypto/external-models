{% docs __overview__ %}

# Welcome to the Flipside Crypto External Models Documentation!

## **What does this documentation cover?**
The documentation included here details the design of the External tables and views available via [Flipside Crypto](https://flipsidecrypto.xyz/). The models in the External database leverage non-Flipside curated datasets and APIs. While Flipside has the ability to host these datasets, we do not have authority over the data quality or structure of the outputs. For more information on how these models are built, please see [the github repository.](https://github.com/FlipsideCrypto/external-models)

### **Quick Links to Table Documentation**

**Token Flow: Starknet**

Note: These tables ceased updating on Feburary 4th, 2024.

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

**DefiLlama**

[DefiLlama Documentation](https://defillama.com/docs/api)

- [defillama__dim_bridges](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.defillama__dim_bridges)
- [defillama__dim_chains](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.defillama__dim_chains)
- [defillama__dim_dexes](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.defillama__dim_dexes)
- [defillama__dim_pools](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.defillama__dim_pools)
- [defillama__dim_protocols](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.defillama__dim_protocols)
- [defillama__dim_stablecoins](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.defillama__dim_stablecoins)
- [defillama__fact_bridge_volume](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.defillama__fact_bridge_volume)
- [defillama__fact_bridge_volume_by_chain](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.defillama__fact_bridge_volume_by_chain)
- [defillama__fact_chain_tvl](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.defillama__fact_chain_tvl)
- [defillama__fact_dex_volume](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.defillama__fact_dex_volume)
- [defillama__fact_options_volume](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.defillama__fact_options_volume)
- [defillama__fact_protocol_fees_revenue](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.defillama__fact_protocol_fees_revenue)
- [defillama__fact_stablecoin_supply](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.defillama__fact_stablecoin_supply)
- [defillama__fact_protocol_tvl](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.defillama__fact_protocol_tvl)
- [defillama__fact_pool_yields](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.defillama__fact_pool_yields)
- [defillama__fact_protocol_fees_revenue](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.defillama__fact_protocol_fees_revenue) 

**DeepNFTValue**

[DeepNFTValue Documentation](https://deepnftvalue.readme.io/reference/getting-started-with-deepnftvalue-api)

- [deepnftvalue__fact_collections](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.deepnftvalue__fact_collections)
- [deepnftvalue__fact_tokens](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.deepnftvalue__fact_tokens)
- [deepnftvalue__fact_valuations](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.deepnftvalue__fact_valuations)

**Snapshot**

[Snapshot Documentation](https://docs.snapshot.org/)

- [snapshot__ez_snapshot](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.snapshot__ez_snapshot)
- [snapshot__dim_spaces](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.snapshot__snapshot__dim_spaces)
- [snapshot__dim_users](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.snapshot__dim_users)
- [snapshot__fact_proposals](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.snapshot__fact_proposals)
- [snapshot__fact_votes](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.snapshot__fact_votes)

**Flashbots**

[Flashbots Documentation](https://docs.flashbots.net/flashbots-protect/overview)

- [flashbots__fact_mevshare_transactions](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.flashbots__fact_mevshare_transactions)
- [flashbots__fact_protect_transactions](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.flashbots__fact_protect_transactions)

**Blast Leaderboard**

[Live Leaderboard Site](https://blast.io/en/leaderboard)

- [blast__fact_leaderboard](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.blast__fact_leaderboard)

**Token Lists**

[Token Lists Documentation](https://tokenlists.org/)

- [tokenlists__ez_verified_tokens](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.tokenlists__ez_verified_tokens)

**LayerZero**

[LayerZero Sybil Reporting Documentation](https://github.com/LayerZero-Labs/sybil-report/?tab=readme-ov-file)

- [layerzero__fact_transactions_snapshot](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.layerzero__fact_transactions_snapshot)

**Farcaster (Neynar)**
- [dim_fids](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.farcaster__dim_fids)
- [dim_fnames](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.farcaster__dim_fnames)
- [dim_profile_with_addresses](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.farcaster__dim_profile_with_addresses)
- [fact_blocks](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.farcaster__fact_blocks)
- [fact_channel_follows](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.farcaster__fact_channel_follows)
- [fact_casts](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.farcaster__fact_casts)
- [fact_links](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.farcaster__fact_links)
- [fact_reactions](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.farcaster__fact_reactions)
- [fact_signers](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.farcaster__fact_signers)
- [fact_storage](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.farcaster__fact_storage)
- [fact_user_data](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.farcaster__fact_user_data)
- [fact_verifications](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.farcaster__fact_verifications)
- [fact_warpcast_power_users](https://flipsidecrypto.github.io/external-models/#!/model/model.external_models.farcaster__fact_warpcast_power_users)

## **Helpful User-Defined Functions (UDFs)**

UDFs are custom functions built by the Flipside team that can be used in your queries to make your life easier. 

Please visit [LiveQuery Functions Overview](https://flipsidecrypto.github.io/livequery-models/#!/overview) for a full list of helpful UDFs.

## **Data Model Overview**

`EXTERNAL` is our blockchain-agonistic database for datasets derived from independently managed, external sources. While these models are built a few different ways, the primary method used is through calling internal functions that leverage curated datasets, such as Token Flow or DefiLlama's API endpoints, to create accessible sql models for the analytics community. These models follow our standard approach, built using three layers of sql models: **bronze, silver, and gold (or core).** However, when the models are built externally (non-Flipside), the naming conventions may vary and models will be placed into schemas based on the source.

- Bronze: Data is loaded in from the source as a view
- Silver: All necessary parsing, filtering, de-duping, and other transformations are done here
- Gold (or core): Final views and tables that are available in Velocity

The dimension tables are sourced from a variety of on-chain and off-chain sources.

Convenience views (denoted ez_) are a combination of different fact and dimension tables. 


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
- [Flipside](https://flipsidecrypto.xyz/)
- [Velocity](https://app.flipsidecrypto.com/velocity?nav=Discover)
- [Tutorials](https://docs.flipsidecrypto.com/our-data/tutorials)
- [Github](https://github.com/FlipsideCrypto/external-models)
- [What is dbt?](https://docs.getdbt.com/docs/introduction)


{% enddocs %}