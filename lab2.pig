hotel_review =
		LOAD '/user/cloudera/input_lab2/hotel-review.csv'
		USING PigStorage (';')
		AS (id:int, review:chararray, aspect:chararray, category: chararray, sentiment: chararray);
-- DUMP hotel_review;

-- Bai 1:
-- Dua tat ca ky tu ve chu thuong
hotel_review_lower = FOREACH hotel_review GENERATE 
		LOWER(review) as review,
		aspect,
		category,
		sentiment;
-- DUMP hotel_review_lower;

hotel_review_lower = FOREACH hotel_review GENERATE 
		REPLACE(review, '[?!:,.]', '') as review,
		aspect,
		category,
		sentiment;
STORE hotel_review_lower INTO '/user/cloudera/output_bai1_a';

-- Tach cac dong binh luan thanh day cac tu (tu duoc tach ra tu cau theo khoang trang)
words = FOREACH hotel_review_lower GENERATE 
	FLATTEN(TOKENIZE(review)) as word,
	aspect,
	category,
	sentiment;
-- DUMP words;
STORE words INTO '/user/cloudera/output_bai1_b';

-- Loai bo cac stop word
stopwords =
		LOAD '/user/cloudera/input_lab2/stopwords.txt'
		USING PigStorage (';')
		AS (stopword:chararray);
join_words = JOIN words BY word LEFT OUTER, stopwords BY stopword;
filter_words = FILTER join_words BY (stopwords::stopword is NULL);
result_filter = FOREACH filter_words GENERATE  
		words::word as word,
		words::aspect as aspect,
		words::category as category,
		words::sentiment as sentiment;
-- DUMP result_filter;
STORE result_filter INTO '/user/cloudera/output_bai1_c';

-- Bai 2:
-- Thong ke tan suat xuat hien cua tu. 
word_group = GROUP words BY word;
word_group = FOREACH word_group GENERATE 
		group, 
		COUNT(words) as count;

-- Chi ra 5 tu co tan so xuat hien nhieu nhat.
word_group = ORDER word_group BY count DESC;
top5_words = LIMIT word_group 5;
-- DUMP top5_words;
STORE top5_words INTO '/user/cloudera/output_bai2_a';

-- Thong ke so binh luan theo tung phan loai (category)
category_group = GROUP hotel_review_lower BY category;
category_group = FOREACH category_group GENERATE 
		group as category,
		COUNT(hotel_review_lower) as count;
-- DUMP category_group;
STORE category_group INTO '/user/cloudera/output_bai2_b';

-- Thong ke so binh luan theo tung danh gia (aspect)
aspect_group = GROUP hotel_review_lower BY aspect;
aspect_group = FOREACH aspect_group GENERATE
		group as aspect,
		COUNT(hotel_review_lower) as count;
-- DUMP aspect_group;
STORE aspect_group INTO '/user/cloudera/output_bai2_c';

-- Bai 3:
asp_neg_grp = GROUP hotel_review_lower BY (aspect, sentiment);
asp_neg_grp = FOREACH asp_neg_grp GENERATE
		group.aspect,
		group.sentiment,
		COUNT(hotel_review_lower) as count;

-- Khia canh nhan nhieu danh gia tieu cuc nhat (negative)
asp_negative = FILTER asp_neg_grp BY sentiment == 'negative';
asp_negative = ORDER asp_negative BY count DESC;
top_negative = LIMIT asp_negative 1;
-- DUMP top_negative;
STORE top_negative INTO '/user/cloudera/output_bai3_a';

-- Khia canh nhan nhieu danh gia tich cuc nhat (positive)
asp_positive = FILTER asp_neg_grp BY sentiment == 'positive';
asp_positive = ORDER asp_positive BY count DESC;
top_positive = LIMIT asp_positive 1;
-- DUMP top_positive;
STORE top_positive INTO '/user/cloudera/output_bai3_b';

-- Bai 4:
-- Dua vao tung phan loai binh luan. Xac dinh 5 tu mang y nghia tich cuc nhat
neg_words = FILTER result_filter BY sentiment == 'negative';
neg_grp = GROUP neg_words BY (category, word);
neg_grp = FOREACH neg_grp GENERATE 
		group.category,		
		group.word,
		COUNT(neg_words) as count;
category_grp = GROUP neg_grp BY category;
top5_neg = FOREACH category_grp {
		neg_grp_sorted = ORDER neg_grp BY count DESC;
		top_neg = LIMIT neg_grp_sorted 5;
		GENERATE group as category, top_neg;
};
-- DUMP top5_neg;
STORE top5_neg INTO '/user/cloudera/output_bai4_a';

-- Dua vao tung phan loai binh luan. Xac dinh 5 tu mang y nghia tieu cuc nhat
pos_words = FILTER result_filter BY sentiment == 'positive';
pos_grp = GROUP pos_words BY (category, word);
pos_grp = FOREACH pos_grp GENERATE 
		group.category, 
		group.word, 
		COUNT(pos_words) as count;
category_grp = GROUP pos_grp BY category;
top5_pos = FOREACH category_grp {
		pos_grp_sorted = ORDER pos_grp BY count DESC;
		top_neg = LIMIT pos_grp_sorted 5;
		GENERATE group as category, top_neg;
};
-- DUMP top5_pos;
STORE top5_pos INTO '/user/cloudera/output_bai4_b';

-- Bai 5:
-- Dua vao tung phan loai binh luan. Xac dinh 5 tu lien quan nhat
word_freq = GROUP result_filter BY (category, word);
word_freq = FOREACH word_freq GENERATE
		group.category,
		group.word,
		COUNT(result_filter) as count;
word_freq_cat = GROUP word_freq BY category;
top5_word_freq = FOREACH word_freq_cat {
		word_freq_order = ORDER word_freq BY count DESC;
		top_word_freq = LIMIT word_freq_order 5;
		GENERATE group as category, top_word_freq;
};
DUMP top5_word_freq;
STORE top5_word_freq INTO '/user/cloudera/output_bai5';
