-- Load dữ liệu
reviews = LOAD '/input_lab2/hotel-review.csv'
    USING PigStorage(';')
    AS (id:int, comment:chararray, aspect_category:chararray, aspect:chararray, sentiment:chararray);

-- ================================================================
-- 1. KHÍA CẠNH NHẬN NHIỀU ĐÁNH GIÁ TIÊU CỰC NHẤT (negative)
-- ================================================================

-- Lọc chỉ lấy các dòng có sentiment = negative
negative_reviews = FILTER reviews BY sentiment == 'negative';

-- Distinct theo (id, aspect) để tránh đếm trùng 1 bình luận
neg_id_aspect = FOREACH negative_reviews GENERATE id, aspect;
neg_id_aspect_distinct = DISTINCT neg_id_aspect;

-- Nhóm và đếm theo aspect
neg_grouped = GROUP neg_id_aspect_distinct BY aspect;
neg_count = FOREACH neg_grouped GENERATE
    group AS aspect,
    COUNT(neg_id_aspect_distinct) AS num_negative;

-- Sắp xếp giảm dần → lấy top 1
neg_sorted = ORDER neg_count BY num_negative DESC;
top_negative = LIMIT neg_sorted 1;

-- DUMP top_negative;
STORE top_negative INTO '/output_lab2/top_negative_aspect' USING PigStorage('\t');


-- ================================================================
-- 2. KHÍA CẠNH NHẬN NHIỀU ĐÁNH GIÁ TÍCH CỰC NHẤT (positive)
-- ================================================================

-- Lọc chỉ lấy các dòng có sentiment = positive
positive_reviews = FILTER reviews BY sentiment == 'positive';

-- Distinct theo (id, aspect) để tránh đếm trùng
pos_id_aspect = FOREACH positive_reviews GENERATE id, aspect;
pos_id_aspect_distinct = DISTINCT pos_id_aspect;

-- Nhóm và đếm theo aspect
pos_grouped = GROUP pos_id_aspect_distinct BY aspect;
pos_count = FOREACH pos_grouped GENERATE
    group AS aspect,
    COUNT(pos_id_aspect_distinct) AS num_positive;

-- Sắp xếp giảm dần → lấy top 1
pos_sorted = ORDER pos_count BY num_positive DESC;
top_positive = LIMIT pos_sorted 1;

-- DUMP top_positive;
STORE top_positive INTO '/output_lab2/top_positive_aspect' USING PigStorage('\t');


