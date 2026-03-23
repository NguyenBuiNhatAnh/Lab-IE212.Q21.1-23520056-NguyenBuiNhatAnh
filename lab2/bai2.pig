-- Load dữ liệu
reviews = LOAD '/input_lab2/hotel-review.csv'
    USING PigStorage(';')
    AS (id:int, comment:chararray, aspect_category:chararray, aspect:chararray, sentiment:chararray);

-- ================================================================
-- 1. THỐNG KÊ TẦN SỐ XUẤT HIỆN CỦA TỪ (Top 5)
-- ================================================================

-- Tách từng từ trong comment thành các tuple riêng lẻ
words_raw = FOREACH reviews GENERATE FLATTEN(TOKENIZE(LOWER(comment))) AS word;

-- Lọc token rỗng + loại bỏ token chỉ toàn dấu câu ASCII
words_clean = FILTER words_raw BY
    SIZE(word) > 0
    AND NOT word MATCHES '[\\.,!?;:\\-\\"\\(\\)0-9]+';

-- Nhóm theo từng từ
words_grouped = GROUP words_clean BY word;

-- Đếm tần số mỗi từ
word_count = FOREACH words_grouped GENERATE
    group AS word,
    COUNT(words_clean) AS freq;

-- Sắp xếp giảm dần theo tần số
word_sorted = ORDER word_count BY freq DESC;

-- Lấy top 5
top5_words = LIMIT word_sorted 5;

-- DUMP top5_words;
STORE top5_words INTO '/output_lab2/top5_words' USING PigStorage('\t');


-- ================================================================
-- 2. THỐNG KÊ SỐ BÌNH LUẬN THEO TỪNG PHÂN LOẠI (category)
-- ================================================================

-- Lấy id và aspect_category, loại trùng lặp (mỗi bình luận 1 id)
-- Vì 1 comment có thể có nhiều dòng (nhiều nhãn), ta distinct theo (id, aspect_category)
id_category = FOREACH reviews GENERATE id, aspect_category;
id_category_distinct = DISTINCT id_category;

-- Nhóm theo aspect_category
category_grouped = GROUP id_category_distinct BY aspect_category;

-- Đếm số bình luận mỗi category
category_count = FOREACH category_grouped GENERATE
    group AS category,
    COUNT(id_category_distinct) AS num_comments;

-- Sắp xếp giảm dần
category_sorted = ORDER category_count BY num_comments DESC;

-- DUMP category_sorted;
STORE category_sorted INTO '/output_lab2/category_count' USING PigStorage('\t');


-- ================================================================
-- 3. THỐNG KÊ SỐ BÌNH LUẬN THEO TỪNG KHÍA CẠNH (aspect)
-- ================================================================

-- Lấy id và aspect, distinct để tránh đếm trùng
id_aspect = FOREACH reviews GENERATE id, aspect;
id_aspect_distinct = DISTINCT id_aspect;

-- Nhóm theo aspect
aspect_grouped = GROUP id_aspect_distinct BY aspect;

-- Đếm số bình luận mỗi aspect
aspect_count = FOREACH aspect_grouped GENERATE
    group AS aspect,
    COUNT(id_aspect_distinct) AS num_comments;

-- Sắp xếp giảm dần
aspect_sorted = ORDER aspect_count BY num_comments DESC;

-- DUMP aspect_sorted;
STORE aspect_sorted INTO '/output_lab2/aspect_count' USING PigStorage('\t');