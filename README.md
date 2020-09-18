# MapReduce: Simple Text Indexer

## 1. Build Vocabulary and Index

```bash
hadoop jar SearchEngine.jar VocabIndex input_files output_vocab output_index
```

## 2. Query

```bash
hadoop jar SearchEngine.jar Query input_index output_query number query_string
```
