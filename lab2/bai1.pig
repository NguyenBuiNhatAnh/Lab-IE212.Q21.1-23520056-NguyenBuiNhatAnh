-- ================================================================
-- Đọc stopwords
-- ================================================================
--    PigStorage('\n') hiểu '\n' là ký tự phân tách field (literal),
--    TextLoader() mới đọc đúng từng dòng = 1 stopword
stopwords = LOAD '/input_lab2/stopwords.txt'
    USING TextLoader() AS (stopword:chararray);

-- ================================================================
-- Đọc dữ liệu
-- ================================================================
reviews = LOAD '/input_lab2/hotel-review.csv'
    USING PigStorage(';')
    AS (id:int, comment:chararray, aspect_category:chararray, aspect:chararray, sentiment:chararray);

-- ================================================================
-- Bước 1: Lowercase
-- ================================================================
reviews_lower = FOREACH reviews GENERATE
    id,
    LOWER(comment)  AS comment,
    aspect_category,
    aspect,
    sentiment;

-- ================================================================
-- Bước 2: Loại bỏ dấu câu — giữ chữ cái Unicode + khoảng trắng
-- ================================================================
reviews_clean = FOREACH reviews_lower GENERATE
    id,
    REPLACE(comment, '[^\\p{L}\\s]', '')  AS comment,
    aspect_category,
    aspect,
    sentiment;

-- ================================================================
-- Bước 3: Tokenize → tách câu thành từng từ
-- ================================================================
tokens = FOREACH reviews_clean GENERATE
    id,
    TOKENIZE(comment)  AS words,
    aspect_category,
    aspect,
    sentiment;

-- Flatten: mỗi từ thành 1 dòng riêng
words_flat = FOREACH tokens GENERATE
    id,
    FLATTEN(words)  AS word,
    aspect_category,
    aspect,
    sentiment;

-- ================================================================
-- Bước 4: Loại bỏ stopwords (anti-join pattern)
-- ================================================================
-- LEFT OUTER JOIN → giữ tất cả words_flat
-- IS NULL         → chỉ giữ từ KHÔNG có trong stopwords
joined = JOIN words_flat BY word LEFT OUTER, stopwords BY stopword;

clean_words = FILTER joined BY stopwords::stopword IS NULL;

-- ================================================================
-- Kết quả cuối
-- ================================================================
result = FOREACH clean_words GENERATE
    id,
    words_flat::word            AS word,
    words_flat::aspect_category AS aspect_category,
    words_flat::aspect          AS aspect,
    words_flat::sentiment       AS sentiment;

-- DUMP result;
-- Xuất ra file
STORE result INTO '/output_lab2/bai1_result' USING PigStorage('\t');