-- Gold Layer Schema: OMOP CDM Aligned
-- Run against: sepsis_gold database (PostgreSQL)

-- Person: patient demographics (OMOP standard)
CREATE TABLE IF NOT EXISTS person (
    person_id BIGINT PRIMARY KEY,        
    gender_source_value VARCHAR(10),
    birth_datetime TIMESTAMP,
    death_datetime TIMESTAMP,
    race_source_value VARCHAR(50),
    ethnicity_source_value VARCHAR(50)
);

-- Visit Occurrence: ICU stays (OMOP standard)
CREATE TABLE IF NOT EXISTS visit_occurrence (
    visit_occurrence_id BIGINT PRIMARY KEY, 
    person_id BIGINT REFERENCES person(person_id),
    hadm_id BIGINT,                         
    visit_start_datetime TIMESTAMP NOT NULL,
    visit_end_datetime TIMESTAMP,
    visit_source_value VARCHAR(50),         
    preceding_visit_occurrence_id BIGINT
);

-- Measurement: vitals and labs (OMOP standard)
CREATE TABLE IF NOT EXISTS measurement (
    measurement_id BIGSERIAL PRIMARY KEY,
    person_id BIGINT REFERENCES person(person_id),
    visit_occurrence_id BIGINT REFERENCES visit_occurrence(visit_occurrence_id),
    measurement_datetime TIMESTAMP NOT NULL,
    measurement_source_value VARCHAR(100),    
    value_as_number FLOAT,
    unit_source_value VARCHAR(50),
    -- LOINC mapping for standardization
    measurement_concept_id BIGINT,          
    loinc_code VARCHAR(20)
);

-- Drug Exposure: vasopressors and antibiotics (OMOP standard)
CREATE TABLE IF NOT EXISTS drug_exposure (
    drug_exposure_id BIGSERIAL PRIMARY KEY,
    person_id BIGINT REFERENCES person(person_id),
    visit_occurrence_id BIGINT REFERENCES visit_occurrence(visit_occurrence_id),
    drug_exposure_start_datetime TIMESTAMP NOT NULL,
    drug_exposure_end_datetime TIMESTAMP,
    drug_source_value VARCHAR(200),           
    route_source_value VARCHAR(50),
    dose_value FLOAT,
    dose_unit_source_value VARCHAR(50),
    -- RxNorm mapping for standardization
    drug_concept_id BIGINT,
    rxnorm_code VARCHAR(20)
);

-- Condition Occurrence: sepsis diagnosis flags (OMOP standard)
CREATE TABLE IF NOT EXISTS condition_occurrence (
    condition_occurrence_id BIGSERIAL PRIMARY KEY,
    person_id BIGINT REFERENCES person(person_id),
    visit_occurrence_id BIGINT REFERENCES visit_occurrence(visit_occurrence_id),
    condition_start_datetime TIMESTAMP NOT NULL,
    condition_source_value VARCHAR(100),      
    condition_status INTEGER
);


-- PROJECT-SPECIFIC TABLES (not OMOP standard)

-- Sepsis Risk Scores: ML model output per scoring window
CREATE TABLE IF NOT EXISTS sepsis_risk_scores (
    score_id BIGSERIAL PRIMARY KEY,
    visit_occurrence_id BIGINT REFERENCES visit_occurrence(visit_occurrence_id),
    person_id BIGINT REFERENCES person(person_id),
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    risk_score FLOAT NOT NULL,                -- 0.0 to 1.0
    alert_level VARCHAR(10) NOT NULL,         -- green/yellow/orange/red
    -- SHAP explanations stored as JSON
    shap_values JSONB,
    -- top 3 contributing features
    top_feature_1 VARCHAR(100),
    top_feature_1_value FLOAT,
    top_feature_2 VARCHAR(100),
    top_feature_2_value FLOAT,
    top_feature_3 VARCHAR(100),
    top_feature_3_value FLOAT,
    -- LLM-generated clinical summary (populated by Bedrock)
    bedrock_summary TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Pipeline Metrics: observability table
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    dag_id VARCHAR(100) NOT NULL,
    run_id VARCHAR(100),
    task_id VARCHAR(100),
    status VARCHAR(20) NOT NULL,              -- success/failed/skipped
    rows_processed INTEGER DEFAULT 0,
    rows_failed INTEGER DEFAULT 0,
    ge_failed_expectations INTEGER DEFAULT 0, -- Great Expectations failures
    runtime_seconds FLOAT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_risk_scores_visit ON sepsis_risk_scores(visit_occurrence_id);
CREATE INDEX IF NOT EXISTS idx_risk_scores_window ON sepsis_risk_scores(window_start);
CREATE INDEX IF NOT EXISTS idx_risk_scores_alert ON sepsis_risk_scores(alert_level);
CREATE INDEX IF NOT EXISTS idx_measurement_visit ON measurement(visit_occurrence_id);
CREATE INDEX IF NOT EXISTS idx_measurement_time ON measurement(measurement_datetime);
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_dag ON pipeline_metrics(dag_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_time ON pipeline_metrics(created_at);
