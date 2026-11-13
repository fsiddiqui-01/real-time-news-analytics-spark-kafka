"""
Part 2.1 and 2.2: train a news topic classifier on AG News with Spark MLlib
"""

import argparse

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import (
    IDF,
    CountVectorizer,
    RegexTokenizer,
    StopWordsRemover,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# AG News uses numbers for its classes. This list turns them into names.
LABEL_NAMES = ["World", "Sports", "Business", "Sci/Tech"]


def add_label_name(df, number_col, name_col):
    """Add a column with the class name (World, Sports...) for a class number."""
    names = F.array(*[F.lit(n) for n in LABEL_NAMES])
    return df.withColumn(
        name_col, F.element_at(names, F.col(number_col).cast("int") + 1)
    )


def build_pipeline(vocab_size, min_df, reg_param):
    # a. Split text into lowercase words. The pattern splits on anything that is not a letter, so punctuation and numbers are dropped.

    tokenizer = RegexTokenizer(
        inputCol="text",
        outputCol="words",
        pattern="[^a-zA-Z]+",
        toLowercase=True,
        minTokenLength=2,
    )

    # b. Remove common English words such as "the" and "and".
    remover = StopWordsRemover(inputCol="words", outputCol="filtered")

    # c. TF-IDF. CountVectorizer counts each word in each article (TF),

    counter = CountVectorizer(
        inputCol="filtered",
        outputCol="tf",
        vocabSize=vocab_size,
        minDF=min_df,
    )
    idf = IDF(inputCol="tf", outputCol="features")

    # d. Logistic regression learns a weight for every word and class, then predicts the class with the highest probability.
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=100,
        regParam=reg_param,
        family="multinomial",
    )

    return Pipeline(stages=[tokenizer, remover, counter, idf, lr])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--train", required=True,
                        help="Path to the AG News training parquet file")
    parser.add_argument("--test",
                        help="Optional AG News test parquet file for accuracy")
    parser.add_argument("--model-out", required=True,
                        help="Folder to save the trained PipelineModel")
    parser.add_argument("--vocab-size", type=int, default=50000)
    parser.add_argument("--min-df", type=int, default=5)
    parser.add_argument("--reg-param", type=float, default=0.01)
    parser.add_argument("--driver-memory", default="2g",
                        help="Memory for Spark, e.g. 1g, 2g, 4g")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("CS4371-AGNews-Training")
        .master("local[2]")
        .config("spark.driver.memory", args.driver_memory)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # 2.1 Load AG News and count articles per class
    train = spark.read.parquet(args.train).select("text", "label")
    train = train.withColumn("label", F.col("label").cast("double"))

    print(f"\nAG News training articles: {train.count()}")
    print("Articles per class:")
    counts = add_label_name(train.groupBy("label").count(), "label", "class")
    counts.select("label", "class", "count").orderBy("label").show()

    # 2.2 Build the pipeline, fit it, save it
    pipeline = build_pipeline(args.vocab_size, args.min_df, args.reg_param)
    print("Training the pipeline (this can take a few minutes)...")
    model = pipeline.fit(train)

    model.write().overwrite().save(args.model_out)
    print(f"Saved PipelineModel to: {args.model_out}")

    vocab = model.stages[2].vocabulary
    print(f"Vocabulary size used: {len(vocab)}")

    # Optional: check accuracy on the AG News test set 
    if args.test:
        test = spark.read.parquet(args.test).select("text", "label")
        test = test.withColumn("label", F.col("label").cast("double"))
        predictions = model.transform(test)

        for metric in ["accuracy", "f1"]:
            evaluator = MulticlassClassificationEvaluator(
                labelCol="label", predictionCol="prediction", metricName=metric
            )
            print(f"Test {metric}: {evaluator.evaluate(predictions):.4f}")

        print("\nConfusion matrix (rows = true class, columns = predicted):")
        named = add_label_name(predictions, "label", "true_class")
        named = add_label_name(named, "prediction", "predicted_class")
        (named.groupBy("true_class")
              .pivot("predicted_class", LABEL_NAMES)
              .count()
              .fillna(0)
              .orderBy("true_class")
              .show())

    spark.stop()


if __name__ == "__main__":
    main()
