CREATE TABLE IF NOT EXISTS profile_batches (
    id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    dataset VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    source_release VARCHAR(64) NOT NULL,
    rule_version VARCHAR(64) NOT NULL,
    observation DATE NOT NULL,
    window_days INT NOT NULL,
    content_hash CHAR(64) NOT NULL,
    customer_count BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX dataset_batches(dataset,observation,id)
);
CREATE TABLE IF NOT EXISTS profile_current (
    dataset VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    batch_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    FOREIGN KEY(batch_id) REFERENCES profile_batches(id)
);
CREATE TABLE IF NOT EXISTS customer_profiles (
    batch_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    customer_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    country VARCHAR(255),
    recency_days INT NOT NULL,
    orders BIGINT NOT NULL,
    purchase_amount DECIMAL(30,6) NOT NULL,
    cancellation_amount DECIMAL(30,6) NOT NULL,
    preferred_product VARCHAR(64),
    r_score INT NOT NULL,
    f_score INT NOT NULL,
    m_score INT NOT NULL,
    segment VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    PRIMARY KEY(batch_id,customer_id),
    INDEX segment_page(batch_id,segment,customer_id),
    FOREIGN KEY(batch_id) REFERENCES profile_batches(id)
);
CREATE TABLE IF NOT EXISTS score_batches (
    id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    profile_batch_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    model_id VARCHAR(64) NOT NULL,
    algorithm VARCHAR(64) NOT NULL,
    horizon_days INT NOT NULL,
    content_hash CHAR(64) NOT NULL,
    customer_count BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY score_profile(id,profile_batch_id),
    FOREIGN KEY(profile_batch_id) REFERENCES profile_batches(id)
);
CREATE TABLE IF NOT EXISTS score_current (
    profile_batch_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    batch_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    FOREIGN KEY(profile_batch_id) REFERENCES profile_batches(id),
    FOREIGN KEY(batch_id,profile_batch_id) REFERENCES score_batches(id,profile_batch_id)
);
CREATE TABLE IF NOT EXISTS customer_scores (
    batch_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    profile_batch_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    customer_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    score DECIMAL(13,12) NOT NULL,
    PRIMARY KEY(batch_id,customer_id),
    FOREIGN KEY(batch_id,profile_batch_id) REFERENCES score_batches(id,profile_batch_id),
    FOREIGN KEY(profile_batch_id,customer_id) REFERENCES customer_profiles(batch_id,customer_id)
);
