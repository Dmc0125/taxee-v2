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

-- intermediary table for events errors and transactions
--
-- one to one with `transaction`
-- one to many events if it is an onchain transaction
-- one to one event if it is a custom event
-- one to many errors
create table internal_tx (
    id serial primary key,
    position real not null,

    user_account_id integer not null,
    foreign key (user_account_id) references user_account (id) on delete cascade,

    network network not null,
    tx_id varchar,
    foreign key (user_account_id, tx_id) references tx_ref (
        user_account_id, tx_id
    ) on delete cascade,

    timestamp timestamptz not null
);

create index on internal_tx (position);

create table event (
    id uuid primary key,
    position real not null,

    internal_tx_id integer not null,
    foreign key (internal_tx_id) references internal_tx (id) on delete cascade,

    ui_app_name varchar not null,
    ui_method_name varchar not null,
    type smallint not null,
    data jsonb not null
);

create table parser_error (
    id serial primary key,

    internal_tx_id integer not null,
    foreign key (internal_tx_id) references internal_tx (id) on delete cascade,

    ix_idx integer,

    origin smallint not null,
    type smallint not null,
    data jsonb
);

commit;
