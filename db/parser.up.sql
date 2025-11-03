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

create type event_type as enum (
    'transfer',
    'transfer_internal',
    'mint',
    'burn'
);

create table event (
    user_account_id integer not null,

    -- TODO: later, custom events will be a thing so this should be reviewd
    tx_id varchar not null,
    network network not null,
    ix_idx integer,
    idx integer not null,
    timestamp timestamptz not null,

    -- TODO: review this later
    primary key (user_account_id, tx_id, network, ix_idx, idx),

    foreign key (user_account_id, tx_id) references tx_ref (
        user_account_id, tx_id
    ) on delete cascade,

    ui_app_name varchar not null,
    ui_method_name varchar not null,
    type event_type not null,

    data jsonb not null
);

create type parser_err_origin as enum (
    'preparse',
    'parse'
);

create type parser_err_type as enum (
    'missing_account',
    'account_balance_mismatch',
    'missing_price',
    'insufficient_balance'
);

create table parser_err (
    id serial primary key,
    user_account_id integer not null,

    -- err related to tx
    tx_id varchar not null,
    foreign key (user_account_id, tx_id) references tx_ref (
        user_account_id, tx_id
    ) on delete cascade,
    -- NOTE: could be eth -> does not have concept of ixs
    ix_idx integer,
    unique (user_account_id, tx_id, ix_idx),

    -- -- err related to event
    -- event_id integer,
    -- foreign key (user_account_id, event_id) references event (
    --     user_account_id, id
    -- ) on delete cascade,

    origin parser_err_origin not null,
    type parser_err_type not null,
    data jsonb not null
);

create procedure dev_delete_parsed(
    p_user_account_id integer
)
language plpgsql
as $$
declare
begin
    delete from parser_err where user_account_id = p_user_account_id;
    delete from event where user_account_id = p_user_account_id;
end;
$$;

commit;
