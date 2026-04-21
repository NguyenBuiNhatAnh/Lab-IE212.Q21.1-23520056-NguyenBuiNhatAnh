import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

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
        
sys.stdout = Tee("bai5.txt", "w")

spark = SparkSession.builder \
    .appName("Fecom_Data_Analysis_Lab4_Bai5") \
    .getOrCreate()

print("\n========== BÀI 5: THỐNG KÊ ĐÁNH GIÁ ĐƠN HÀNG ==========\n")

order_reviews_df = spark.read.csv("/lab4/Order_Reviews.csv", header=True, inferSchema=True, sep=";")

# 1. Xử lý dữ liệu: Lọc bỏ NULL và các giá trị ngoại lệ (chỉ giữ lại điểm từ 1 đến 5)
cleaned_reviews_df = order_reviews_df.filter(
    col("Review_Score").isNotNull() & 
    (col("Review_Score") >= 1) & 
    (col("Review_Score") <= 5)
)

# 2. Tính điểm đánh giá trung bình
# Sử dụng hàm avg() và lấy kết quả ra khỏi DataFrame
avg_score = cleaned_reviews_df.select(avg("Review_Score")).collect()[0][0]

# Làm tròn 2 chữ số thập phân cho dễ nhìn
print(f"- Điểm đánh giá trung bình của toàn bộ hệ thống: {round(avg_score, 2)} / 5.0\n")

# 3. Thống kê số lượng đánh giá theo từng mức (từ 1 đến 5)
print("- Số lượng đánh giá phân bổ theo từng mức điểm:")
score_distribution_df = cleaned_reviews_df.groupBy("Review_Score") \
    .agg(count("Review_ID").alias("Total_Reviews")) \
    .orderBy("Review_Score")  # Sắp xếp từ 1 sao đến 5 sao

# In ra kết quả
score_distribution_df.show()

print("\n=======================================================")