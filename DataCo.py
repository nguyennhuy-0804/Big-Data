from pyspark.sql import SparkSession

# Tạo Spark session
spark = SparkSession.builder.appName("DropColumns").getOrCreate()

# Đọc file CSV gốc
df = spark.read.csv(r"C:\Users\ASUS\Downloads\DataCoSupplyChainDataset.csv", header=True, inferSchema=True)

# Xử lý: Bỏ 2 cột + loại bỏ dòng thiếu dữ liệu
df_cleaned = df.drop("Order Zipcode", "Product Description") \
               .na.drop(subset=["Customer Lname", "Customer Zipcode"])

# Ghi lại file đã làm sạch (lưu trong thư mục DataCo_Cleaned)
df_cleaned.coalesce(1).write.csv("DataCo_Cleaned", header=True, mode="overwrite")

# In schema và 5 dòng đầu
df_cleaned.printSchema()
df_cleaned.show(5, truncate=False)
