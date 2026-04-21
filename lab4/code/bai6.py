import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, col, sum, round

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
        
sys.stdout = Tee("bai6.txt", "w")

spark = SparkSession.builder \
    .appName("Fecom_Data_Analysis_Lab4_Bai6") \
    .getOrCreate()

print("\n========== BÀI 6: DOANH THU THEO DANH MỤC TRONG NĂM 2024 ==========\n")

order_items_df = spark.read.csv("/lab4/Order_Items.csv", header=True, inferSchema=True, sep=";")
orders_df = spark.read.csv("/lab4/Orders.csv", header=True, inferSchema=True, sep=";")
products_df = spark.read.csv("/lab4/Products.csv", header=True, inferSchema=True, sep=";")

# 1. Lọc các đơn hàng được đặt trong năm 2024
orders_2024 = orders_df.filter(year(col("Order_Purchase_Timestamp")) == 2024)

# 2. Thực hiện chuỗi kết nối (Join) các bảng:
# Orders -> Order_Items (qua Order_ID)
# Kết quả -> Products (qua Product_ID)
revenue_df = orders_2024.join(order_items_df, on="Order_ID", how="inner") \
    .join(products_df, on="Product_ID", how="inner")

# 3. Tính tổng doanh thu (Price + Freight_Value) cho từng dòng
# Sau đó gom nhóm theo danh mục và tính tổng doanh thu của danh mục đó
category_revenue_2024 = revenue_df.withColumn("Item_Total", col("Price") + col("Freight_Value")) \
    .groupBy("Product_Category_Name") \
    .agg(round(sum("Item_Total"), 2).alias("Total_Revenue")) \
    .orderBy(col("Total_Revenue").desc())

# 4. Hiển thị toàn bộ danh sách kết quả
print(f"Thống kê doanh thu trên tổng số {category_revenue_2024.count()} danh mục:")
category_revenue_2024.show(category_revenue_2024.count(), truncate=False)

print("\n====================================================================")