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
    coingecko_id varchar not null,
    foreign key (coingecko_id) references coingecko_token_data (coingecko_id) on delete cascade,

    timestamp_from timestamptz not null,
    timestamp_to timestamptz not null,

    primary key (coingecko_id, timestamp_from, timestamp_to)
);

create procedure set_missing_pricepoint(
    p_coingecko_id varchar,
    p_timestamp_from timestamptz,
    p_timestamp_to timestamptz
)
language plpgsql
as $$
declare
    v_overlaps_found boolean := false;
    v_min_from timestamptz := p_timestamp_from;
    v_max_to timestamptz := p_timestamp_to;
begin
    select 
        true,
        least(min(timestamp_from), p_timestamp_from),
        greatest(max(timestamp_to), p_timestamp_to)
    into
        v_overlaps_found, v_min_from, v_max_to
    from
        missing_pricepoint
    where
        coingecko_id = p_coingecko_id and not (
            -- non overlapping
            p_timestamp_from > timestamp_to or p_timestamp_to < timestamp_from
        );

    if v_overlaps_found then
        delete from missing_pricepoint where 
            coingecko_id = p_coingecko_id and not (
                -- non overlapping
                p_timestamp_from > timestamp_to or p_timestamp_to < timestamp_from
            );
    end if;

    insert into missing_pricepoint (
        coingecko_id, timestamp_from, timestamp_to
    ) values (
        p_coingecko_id, v_min_from, v_max_to
    );
end;
$$;

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

    tx_id varchar,
    foreign key (tx_id) references tx (id) on delete cascade
);

create index on internal_tx (position);

create table event (
    id uuid primary key,
    position real not null,

    internal_tx_id integer not null,
    foreign key (internal_tx_id) references internal_tx (id) on delete cascade,

    app varchar not null,
    method varchar not null,
    type smallint not null
);

create table event_transfer (
    id uuid primary key,
    event_id uuid not null,
    foreign key (event_id) references event (id) on delete cascade,
    position integer not null,

    -- parser

    -- 0 => incoming 
    -- 1 => outgoing
    -- 2 => internal
    direction smallint not null,
    from_wallet varchar,
    from_account varchar,
    to_wallet varchar,
    to_account varchar,

    token varchar not null,
    amount varchar not null,
    -- has to be int because MaxUint16 > MaxInt16
    token_source integer not null,

    -- inventory
    price varchar,
    value varchar,
    profit varchar,
    missing_amount varchar
);

create table event_transfer_source (
    id serial primary key,
    transfer_id uuid not null,
    source_transfer_id uuid not null,
    used_amount varchar not null,

    -- primary key (transfer_id, source_transfer_id),
    foreign key (transfer_id) references event_transfer (id) on delete cascade,
    foreign key (source_transfer_id) references event_transfer (id) on delete cascade,

    check (transfer_id <> source_transfer_id)
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
