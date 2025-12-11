begin;

create table stats (
    user_account_id integer not null,
    primary key (user_account_id),
    foreign key (user_account_id) references user_account (id) on delete cascade,

    tx_count integer default 0
);

create type network as enum (
    'solana',
    'arbitrum',
    'bsc',
    'avaxc',
    'ethereum'
);

create table wallet (
    id serial primary key,
    user_account_id integer not null,
    foreign key (user_account_id) references user_account (id) on delete cascade,

    address varchar(64) not null,
    network network not null,
    name varchar,

    unique (id, user_account_id),

    tx_count integer default 0,
    data jsonb not null,
    delete_scheduled boolean not null default false,

    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create function dev_set_wallet(
    p_user_account_id integer,
    p_address varchar(64),
    p_network network 
) returns table(
    wallet_id integer,
    wallet_data jsonb
)
language plpgsql
as $$
declare
    wallet_record record;
    new_wallet_id integer;
begin
    select
        w.id, w.data
    into
        wallet_record
    from
        wallet w
    where
        w.address = p_address and
        w.network = p_network and
        w.user_account_id = p_user_account_id;

    if found then
        return query select wallet_record.id, wallet_record.data;
    else
        insert into wallet (
            address, network, user_account_id, data
        ) values (
            p_address, p_network, p_user_account_id, '{}'::jsonb
        ) returning wallet.id into new_wallet_id;

        insert into stats (
            user_account_id, wallets_count
        ) values (p_user_account_id, 1) on conflict (
            user_account_id
        ) do update set
            wallets_count = stats.wallets_count + 1;

        return query select new_wallet_id, '{}'::jsonb;
    end if;
end;
$$;

-- TODO: Add user_account_id
create table solana_related_account (
    wallet_id integer not null,
    address varchar(64) not null,
    primary key (wallet_id, address),
    foreign key (wallet_id) references wallet (id) on delete cascade,
    latest_tx_id varchar
);

create table tx (
    id varchar primary key,
    network network not null,
    err bool not null,
    signer varchar(64) not null,
    fee_payer varchar(64) not null,
    timestamp timestamptz not null,
    -- network = solana -> SolanaTxData
    data jsonb not null
);

-- junction table for:
--
-- user                   <-> tx
-- wallet                 <-> tx
-- solana_related_account <-> tx
create table tx_ref (
    user_account_id integer not null,
    tx_id varchar not null,
    primary key (user_account_id, tx_id),

    wallets integer[],
    related_accounts varchar(64)[],

    foreign key (user_account_id) references user_account (id),
    foreign key (tx_id) references tx (id) on delete cascade
);

create function tx_ref_delete_wallet()
returns trigger
language plpgsql
as $$
declare
begin
    update tx_ref set
        wallets = array_remove(wallets, old.id)
    where
        user_account_id = old.user_account_id;

    return old;
end;
$$;

create trigger tx_ref_wallet_on_delete_cascade
before delete on wallet
for each row
execute function tx_ref_delete_wallet();

create function tx_ref_delete_related_account()
returns trigger
language plpgsql
as $$
declare
begin
    update tx_ref set
        related_accounts = array_remove(tx_ref.related_accounts, old.address)
    where
        old.wallet_id = any(tx_ref.wallets);

    return old;
end;
$$;

create trigger tx_ref_related_accounts_on_delete_cascade
before delete on solana_related_account
for each row
execute function tx_ref_delete_related_account();

commit;
