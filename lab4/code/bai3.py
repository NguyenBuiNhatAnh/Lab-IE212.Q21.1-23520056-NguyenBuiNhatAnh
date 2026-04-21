import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc

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
        
sys.stdout = Tee("bai3.txt", "w")

spark = SparkSession.builder \
    .appName("Fecom_Data_Analysis_Lab4_Bai3") \
    .getOrCreate()

print("\n========== BÀI 3: SỐ LƯỢNG ĐƠN HÀNG THEO QUỐC GIA ==========\n")

orders_df = spark.read.csv("/lab4/Orders.csv", header=True, inferSchema=True, sep=";")
customer_df = spark.read.csv("/lab4/Customer_List.csv", header=True, inferSchema=True, sep=";")

# 1. Kết nối (Join) bảng Orders và Customer_List dựa trên Customer_Trx_ID
# 2. Gom nhóm (groupBy) theo cột Customer_Country
# 3. Đếm (count) số lượng Order_ID và đặt tên cột mới là "Total_Orders"
# 4. Sắp xếp (orderBy) theo cột Total_Orders giảm dần (desc)
orders_by_country_df = orders_df.join(customer_df, on="Customer_Trx_ID", how="inner") \
    .groupBy("Customer_Country") \
    .agg(count("Order_ID").alias("Total_Orders")) \
    .orderBy(desc("Total_Orders"))

orders_by_country_df.show(orders_by_country_df.count(), truncate=False)

print("\n============================================================")