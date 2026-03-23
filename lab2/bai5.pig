-- Load dữ liệu
reviews = LOAD '/input_lab2/hotel-review.csv'
    USING PigStorage(';')
    AS (id:int, comment:chararray, aspect_category:chararray, aspect:chararray, sentiment:chararray);

-- ================================================================
-- TIỀN XỬ LÝ: Tách từ theo từng category
-- ================================================================

-- Distinct (id, comment, aspect_category) tránh đếm trùng do multi-label
id_cat = FOREACH reviews GENERATE id, comment, aspect_category;
id_cat_distinct = DISTINCT id_cat;

-- Tách từ, giữ kèm aspect_category
words_raw = FOREACH id_cat_distinct GENERATE
    FLATTEN(TOKENIZE(LOWER(comment))) AS word,
    aspect_category AS category;

-- Lọc token rỗng + loại bỏ token chỉ toàn dấu câu ASCII
words_clean = FILTER words_raw BY
    SIZE(word) > 0
    AND NOT word MATCHES '[\\.,!?;:\\-\\"\\(\\)0-9]+';

-- ================================================================
-- BƯỚC 1: TF — Tần số từ trong từng category (Term Frequency)
-- ================================================================

-- Nhóm theo (category, word) → đếm số lần xuất hiện
tf_grouped = GROUP words_clean BY (category, word);
tf = FOREACH tf_grouped GENERATE
    FLATTEN(group)              AS (category, word),
    COUNT(words_clean)          AS tf;          -- số lần từ xuất hiện trong category


-- ================================================================
-- BƯỚC 2: DF — Số category chứa từ đó (Document Frequency)
-- ================================================================

-- Lấy danh sách (category, word) duy nhất → mỗi cặp chỉ tính 1 lần
cat_word_distinct = FOREACH tf GENERATE category, word;

-- Nhóm theo word → đếm có bao nhiêu category chứa từ đó
df_grouped = GROUP cat_word_distinct BY word;
df = FOREACH df_grouped GENERATE
    group               AS word,
    COUNT(cat_word_distinct) AS df;             -- số category chứa từ này


-- ================================================================
-- BƯỚC 3: Tổng số category (N) — dùng để tính IDF
-- ================================================================

-- Lấy danh sách category duy nhất
all_cats = FOREACH id_cat_distinct GENERATE aspect_category AS category;
all_cats_distinct = DISTINCT all_cats;

-- Đếm tổng số category
cat_grouped = GROUP all_cats_distinct ALL;
N_relation  = FOREACH cat_grouped GENERATE COUNT(all_cats_distinct) AS N;

-- Cross để gắn N vào từng dòng df
df_with_N = CROSS df, N_relation;

-- Tính IDF = log(N / df)  — dùng LOG() của Pig (log tự nhiên)
idf = FOREACH df_with_N GENERATE
    word,
    df,
    N_relation::N   AS N,
    LOG((double)N_relation::N / (double)df) AS idf;


-- ================================================================
-- BƯỚC 4: TF-IDF = tf * idf — JOIN tf và idf theo word
-- ================================================================

tf_idf_join = JOIN tf BY word, idf BY word;

tf_idf = FOREACH tf_idf_join GENERATE
    tf::category    AS category,
    tf::word        AS word,
    tf::tf          AS tf,
    idf::idf        AS idf,
    (double)tf::tf * idf::idf AS tfidf;         -- điểm TF-IDF cuối cùng


-- ================================================================
-- BƯỚC 5: Sắp xếp và lấy Top 5 từ mỗi category
-- ================================================================

-- Nhóm theo category
tfidf_by_cat = GROUP tf_idf BY category;

-- Với mỗi category: sắp xếp giảm dần theo tfidf, lấy top 5
top5_per_cat = FOREACH tfidf_by_cat {
    sorted = ORDER tf_idf BY tfidf DESC;
    top5   = LIMIT sorted 5;
    GENERATE group AS category, FLATTEN(top5);
};

-- Chỉ giữ các cột cần thiết để output gọn
result = FOREACH top5_per_cat GENERATE
    category,
    tf_idf::word    AS word,
    tf_idf::tfidf   AS tfidf;

result_sorted = ORDER result BY category ASC, tfidf DESC;

-- DUMP result_sorted;
STORE result INTO '/output_lab2/top5_words_by_category' USING PigStorage('\t');
