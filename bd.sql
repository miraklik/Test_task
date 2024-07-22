
CREATE TABLE message (
    id SERIAL PRIMARY KEY,
    content TEXT NOT NULL,
    processed BOOLEAN DEFAULT FALSE
);