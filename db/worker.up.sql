begin;

create type worker_type as enum (
    'fetch_wallet'
);

create table worker_result (
    id serial primary key,

    type worker_type not null,
    data jsonb,

    canceled boolean not null default false,
    error_message varchar,

    started_at timestamptz not null,
    finished_at timestamptz not null
);

commit;
