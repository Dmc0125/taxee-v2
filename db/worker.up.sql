begin;

create table worker_job (
    id uuid primary key,
    user_account_id integer not null,
    foreign key (user_account_id) references user_account (id),

    -- 0 => fetch wallet
    -- 1 => parse transactions
    -- 2 => parse events
    type smallint not null,
    -- 0 => queued
    -- 1 => in progress
    -- 2 => success
    -- 3 => error
    -- 4 => cancel scheduled
    -- 5 => canceled
    status smallint not null default 0,
    -- depends on type: /pkg/db/worker.go
    data jsonb,

    unique (user_account_id, type, data),

    created_at timestamptz not null default now(),
    finished_at timestamptz not null default now()
);

create table worker_error (
    id serial primary key,
    job_id uuid not null,
    data jsonb,
    error_message varchar not null,
    created_at timestamptz not null default now()
);

commit;
