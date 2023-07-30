# Data preprocessing notes

**Paper**: Large Language Models are Zero-Shot Rankers for Recommender Systems  
[[repo](https://github.com/RUCAIBox/LLMRank)]

> One category from the **Amazon Review dataset** named
**Games** where reviews are regarded as interactions. We filter out users and items with fewer than five
interactions. Then we sort the interactions of each user by timestamp, with the oldest interactions
first, to construct the corresponding historical interaction sequences.

[data-preparation.md](https://github.com/RUCAIBox/LLMRank/blob/master/llmrank/dataset/data-preparation.md) - Data preparation description  
[data_process_amazon.py](https://github.com/RUCAIBox/LLMRank/blob/master/llmrank/data_process_amazon.py) - Data preparation script