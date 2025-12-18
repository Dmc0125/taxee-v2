begin;

create type worker_type as enum (
    'fetch_wallet',
    'parse_transactions',
    'parse_events'
);

create table worker_job (
    id serial primary key,
    user_account_id integer not null,
    -- TODO: how to handle user delete
    foreign key (user_account_id) references user_account (id),

    type worker_type not null,
    check (type != 'fetch_wallet'),

    unique (user_account_id, type),

    status status not null,
    queued_at timestamptz,
    finished_at timestamptz
);

create table worker_result (
    id serial primary key,

    type worker_type not null,
    data jsonb,

    status status not null,
    error_message varchar,

    started_at timestamptz not null,

    finished_at timestamptz not null
);

commit;
