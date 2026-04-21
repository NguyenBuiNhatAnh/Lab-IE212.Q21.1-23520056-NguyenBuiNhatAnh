import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, count, asc, desc

# --- THỦ THUẬT: Lớp Tee giúp chuyển hướng output ---
# Class này sẽ bắt mọi lệnh print(), .show(), .printSchema() 
# và đồng thời đẩy ra cả màn hình (terminal) lẫn file txt.
class Tee(object):
    def __init__(self, filename, mode):
        self.file = open(filename, mode, encoding="utf-8")
        self.stdout = sys.stdout
    def write(self, data):
        self.file.write(data)
        self.stdout.write(data)
    def flush(self):
        self.file.flush()
        self.stdout.flush()
        
sys.stdout = Tee("bai4.txt", "w")

spark = SparkSession.builder \
    .appName("Fecom_Data_Analysis_Lab4_Bai4") \
    .getOrCreate()

print("\n========== BÀI 4: PHÂN TÍCH ĐƠN HÀNG THEO THỜI GIAN ==========\n")

orders_df = spark.read.csv("/lab4/Orders.csv", header=True, inferSchema=True, sep=";")

# 1. Trích xuất Năm (Year) và Tháng (Month) từ cột Order_Purchase_Timestamp
# 2. Gom nhóm theo Năm và Tháng
# 3. Đếm số lượng đơn hàng trong mỗi nhóm
# 4. Sắp xếp theo Năm tăng dần (asc) và Tháng giảm dần (desc)
orders_time_analysis_df = orders_df.withColumn("Order_Year", year("Order_Purchase_Timestamp")) \
    .withColumn("Order_Month", month("Order_Purchase_Timestamp")) \
    .groupBy("Order_Year", "Order_Month") \
    .agg(count("Order_ID").alias("Total_Orders")) \
    .orderBy(asc("Order_Year"), desc("Order_Month"))

# Hiển thị toàn bộ kết quả thống kê
orders_time_analysis_df.show(orders_time_analysis_df.count(), truncate=False)

print("\n==============================================================")