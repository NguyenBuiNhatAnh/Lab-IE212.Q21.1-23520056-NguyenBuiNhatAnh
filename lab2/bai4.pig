-- Load dữ liệu
reviews = LOAD '/input_lab2/hotel-review.csv'
    USING PigStorage(';')
    AS (id:int, comment:chararray, aspect_category:chararray, aspect:chararray, sentiment:chararray);

-- ================================================================
-- TIỀN XỬ LÝ CHUNG: Tách từ + chuẩn hóa + gắn nhãn sentiment
-- ================================================================

-- Giữ lại (comment, sentiment), loại trùng theo (id, sentiment)
-- để 1 bình luận không bị đếm từ nhiều lần do nhiều dòng nhãn
id_comment_sentiment = FOREACH reviews GENERATE id, comment, sentiment;
id_comment_distinct  = DISTINCT id_comment_sentiment;

-- Tách từng từ, giữ kèm nhãn sentiment
words_raw = FOREACH id_comment_distinct GENERATE
    FLATTEN(TOKENIZE(LOWER(comment))) AS word,
    sentiment;

-- Lọc token rỗng + loại bỏ token chỉ toàn dấu câu ASCII
words_clean = FILTER words_raw BY
    SIZE(word) > 0
    AND NOT word MATCHES '[\\.,!?;:\\-\\"\\(\\)0-9]+';

-- ================================================================
-- 1. TOP 5 TỪ MANG Ý NGHĨA TÍCH CỰC NHẤT
-- ================================================================

-- Lọc các từ xuất hiện trong bình luận positive
pos_words = FILTER words_clean BY sentiment == 'positive';

-- Nhóm và đếm tần số từng từ
pos_grouped = GROUP pos_words BY word;
pos_freq    = FOREACH pos_grouped GENERATE
    group          AS word,
    COUNT(pos_words) AS freq;

-- Sắp xếp giảm dần, lấy top 5
pos_sorted  = ORDER pos_freq BY freq DESC;
top5_positive = LIMIT pos_sorted 5;

-- DUMP top5_positive;
STORE top5_positive INTO '/output_lab2/top5_positive_words' USING PigStorage('\t');


-- ================================================================
-- 2. TOP 5 TỪ MANG Ý NGHĨA TIÊU CỰC NHẤT
-- ================================================================

-- Lọc các từ xuất hiện trong bình luận negative
neg_words = FILTER words_clean BY sentiment == 'negative';

-- Nhóm và đếm tần số từng từ
neg_grouped = GROUP neg_words BY word;
neg_freq    = FOREACH neg_grouped GENERATE
    group          AS word,
    COUNT(neg_words) AS freq;

-- Sắp xếp giảm dần, lấy top 5
neg_sorted  = ORDER neg_freq BY freq DESC;
top5_negative = LIMIT neg_sorted 5;

-- DUMP top5_negative;
STORE top5_negative INTO '/output_lab2/top5_negative_words' USING PigStorage('\t');
