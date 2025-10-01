begin;

create type err_origin as enum (
    'preparse',
    'parse'
);

create type err_type as enum (
    'account_missing',
    'account_balance_mismatch'
);

create table err (
    id serial primary key,

    tx_id varchar not null,
    -- NOTE: could be eth -> does not have concept of ixs
    ix_idx integer,
    idx integer not null, 

    origin err_origin not null,
    type err_type not null,
    address varchar(64) not null,
    data jsonb, 

    foreign key (tx_id) references tx (id),
    unique (tx_id, ix_idx, idx)
);

commit;
