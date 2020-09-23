CREATE TABLE IF NOT EXISTS public.mutation_message
(
    id      varchar   NOT NULL,
    "type"  varchar   NOT NULL,
    dataset varchar   NOT NULL,
    ts      timestamp NOT NULL,
    CONSTRAINT mutation_message_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.data
(
    id      varchar NOT NULL,
    feature varchar NULL,
    "data"  varchar NULL,
    CONSTRAINT data_pk PRIMARY KEY (id)
);