use spark_connect_rs::{SparkSession, SparkSessionBuilder};

use spark_connect_rs::functions as F;

use spark_connect_rs::dataframe::SaveMode;
use spark_connect_rs::types::DataType;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark: SparkSession = SparkSessionBuilder::default().build().await?;

    let path = "./datasets/people.csv";

    let df = spark
        .read()
        .format("csv")
        .option("header", "True")
        .option("delimiter", ";")
        .load([path])?;

    let df = df
        .select([
            F::col("name"),
            F::col("age").cast(DataType::Integer).alias("age_int"),
        ])
        .sort([F::col("name").desc()]);

    df.clone().show(Some(5), None, None).await?;

    df.write()
        .format("parquet")
        .mode(SaveMode::Overwrite)
        .save("./datasets/people.parquet")
        .await?;

    Ok(())
}
