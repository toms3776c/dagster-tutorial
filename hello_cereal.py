import csv
import os

from dagster import execute_pipeline, pipeline, solid

@solid
def hello_cereal(context):
    # データセットがこのファイルと同じディレクトリにあると仮定
    dataset_path = os.path.join(os.path.dirname(__file__), "cereal.csv")
    with open(dataset_path, "r") as fd:
        # 標準のcsvライブラリを使用して行を読み込む
        cereals = [row for row in csv.DictReader(fd)]

    context.log.info(
        "シリアルに関するデータは{n_cereals}件あります。".format(n_cereals=len(cereals))
    )

    return cereals

@pipeline
def hello_cereal_pipeline():
    hello_cereal()
