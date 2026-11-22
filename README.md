# pyspark-movie-reviews

DAS Module 1 – Homework 1.2: process the IMDb 50K Movie Review dataset with PySpark.

Dataset: https://www.kaggle.com/datasets/lakshmi25npathi/imdb-dataset-of-50k-movie-reviews (place `IMDB Dataset.csv` in `data/`).

## Run
```bash
pip install -r requirements.txt   # needs Java 17
python movie_reviews.py "data/IMDB Dataset.csv" output/cleaned_reviews
```

## What it does
1. Loads the CSV into a DataFrame (multiLine + quote-escape, since reviews contain commas/newlines)
2. Prints schema and first 5 rows
3. Removes null / empty reviews
4. Counts total, positive and negative reviews
5. Adds `review_length` (characters) and shows reviews longer than 500 characters
6. Saves the cleaned + transformed DataFrame as CSV

## Results (from `output/run_log.txt`)
| Metric | Value |
|---|---|
| Rows before cleaning | 50,000 |
| Total reviews after cleaning | 50,000 |
| Positive / Negative | 25,000 / 25,000 |
| Reviews > 500 characters | 45,017 |
| Avg review length (positive / negative) | 1,324.8 / 1,294.1 chars |

`output/sample_output_first100.csv` is a 100-row sample of the saved result (the full file is ~66 MB, so it is git-ignored).

_AI disclosure: AI assistance (Claude) was used in drafting the text and scripts; I reviewed the work and verified the outputs._
