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

create table app_metadata (
    network network not null
    address varchar(64) not null,
    image_url varchar,
    label varchar not null,

    primary key (network, address)
);

create type user_event_type as enum (
    'transfer'
);

create table user_event (
    user_account_id integer not null,
    tx_id varchar not null,
    foreign key (user_account_id, tx_id) references tx_ref (
        user_account_id, tx_id
    ) on delete cascade,

    ix_idx integer,
    idx integer not null, 
    unique (user_account_id, tx_id, ix_idx, idx),

    -- TODO: think of something more ergonomic, some global store of these
    -- things, not sure if makes sense
    network network not null,
    app varchar(64) not null,
    app_method varchar not null,
    foreign key (network, app) references app_metadata (
        network, address
    ) on delete cascade,

    type user_event_type not null,
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
    delete from user_event where user_account_id = p_user_account_id;
end;
$$;

commit;
