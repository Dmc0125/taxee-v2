begin;

-- TODO: the only token info right now is from coingecko, will habe to think
-- about more general way if we have multiple source
create table coingecko_token_data (
    coingecko_id varchar not null primary key,
    name varchar not null,
    symbol varchar not null,
    image_url varchar
);

create table coingecko_token (
    coingecko_id varchar not null,
    foreign key (coingecko_id) references coingecko_token_data (coingecko_id) on delete cascade,
    address varchar not null,
    network network not null,
    primary key (address, network)
);

create table pricepoint (
    coingecko_id varchar not null,
    foreign key (coingecko_id) references coingecko_token_data (coingecko_id) on delete cascade,

    timestamp timestamptz not null,
    price varchar not null,
    primary key (coingecko_id, timestamp)
);

-- TODO: this probaly shouldnt be just for coingecko tokens, idk
create table missing_pricepoint (
    coingecko_id varchar primary key,
    foreign key (coingecko_id) references coingecko_token_data (coingecko_id) on delete cascade,
    
    -- TODO: not sure if this can be like that, if there awould be missing prices
    -- somewhere in the middle, it won't be possible to save that
    timestamp_from timestamptz not null,
    timestamp_to timestamptz not null
);

create table event (
    user_account_id integer not null,
    tx_id varchar not null,
    foreign key (user_account_id, tx_id) references tx_ref (
        user_account_id, tx_id
    ) on delete cascade,

    ix_idx integer,
    idx integer not null default 0,

    primary key (tx_id, ix_idx, idx, user_account_id),

    ui_app_name varchar not null,
    ui_method_name varchar not null,
    type smallint not null,
    data jsonb not null
);

create table parser_error (
    id serial primary key,

    user_account_id integer not null,
    tx_id varchar not null,
    foreign key (user_account_id, tx_id) references tx_ref (
        user_account_id, tx_id
    ) on delete cascade,

    ix_idx integer,
    event_idx integer,
    foreign key (
        user_account_id, tx_id, ix_idx, event_idx
    ) references event (
        user_account_id, tx_id, ix_idx, idx
    ) on delete cascade,

    -- 0 => tx preprocess
    -- 1 => tx process / event process
    origin smallint not null,
    type smallint not null,
    data jsonb not null
);

commit;
