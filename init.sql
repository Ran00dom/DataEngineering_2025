
CREATE TABLE IF NOT EXISTS analytics_data (
    id SERIAL PRIMARY KEY,
    value_name VARCHAR(255) NOT NULL UNIQUE, /* имена уникальны, дублирование строк не возможно*/
    value_array TEXT[]
);



