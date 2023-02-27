create table documents (
  document_id SERIAL PRIMARY KEY,
  ml_response VARCHAR
);


create table parsed_total (
  id SERIAL PRIMARY KEY,
  aggregation VARCHAR
);
