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
    user_account_id integer not null,
    tx_id varchar not null,
    foreign key (user_account_id, tx_id) references tx_ref (
        user_account_id, tx_id
    ) on delete cascade,
    -- NOTE: could be eth -> does not have concept of ixs
    ix_idx integer,
    idx integer not null, 
    unique (user_account_id, tx_id, ix_idx, idx),

    origin err_origin not null,
    type err_type not null,
    address varchar(64) not null,
    data jsonb 
);

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
    network smallint not null,
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
    tx_id varchar not null,
    network smallint not null,
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

create procedure dev_delete_parsed(
    p_user_account_id integer
)
language plpgsql
as $$
declare
begin
    delete from err where user_account_id = p_user_account_id;
    delete from event where user_account_id = p_user_account_id;
end;
$$;

commit;
