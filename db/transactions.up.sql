begin;

create type network as enum ('solana', 'arbitrum');

create table stats (
    user_account_id integer not null,
    primary key (user_account_id),
    foreign key (user_account_id) references user_account (id) on delete cascade,

    -- count of all user txs combined
    -- all wallets, all related accounts
    tx_count integer default 0,
    wallets_count integer default 0
);

create table wallet (
    id serial primary key,
    user_account_id integer not null,
    foreign key (user_account_id) references user_account (id) on delete cascade,

    address varchar(64) not null,
    network network not null,
    name varchar,

    unique (user_account_id, address, network),

    -- count of txs related to **only** this wallet
    tx_count integer default 0,
    -- count of txs related to this wallet and it's related accounts
    total_tx_count integer default 0,
    latest_tx_id varchar
);

create function set_wallet(
    p_user_account_id integer,
    p_address varchar(64),
    p_network network
) returns table(
    wallet_id integer,
    wallet_latest_tx_id varchar
)
language plpgsql
as $$
declare
    wallet_record record;
    new_wallet_id integer;
begin
    select
        w.id, w.latest_tx_id
    into
        wallet_record
    from
        wallet w
    where
        w.address = p_address and
        w.network = p_network and
        w.user_account_id = p_user_account_id;

    if found then
        return query select wallet_record.id, wallet_record.latest_tx_id;
    else
        insert into wallet (
            address, network, user_account_id
        ) values (
            p_address, p_network, p_user_account_id
        ) returning wallet.id into new_wallet_id;

        insert into stats (
            user_account_id, wallets_count
        ) values (p_user_account_id, 1) on conflict (
            user_account_id
        ) do update set
            wallets_count = stats.wallets_count + 1;

        return query select new_wallet_id, null::varchar;
    end if;
end;
$$;

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

create function array_append_unique(
    arr anyarray,
    el anycompatible
) returns anyarray
language plpgsql
immutable
as $$
declare
begin
    if el is null or el = any(arr) then
        return arr;
    else
        return array_append(arr, el);
    end if;
end;
$$;

create procedure set_user_txs_count(
    p_user_account_id integer,
    p_wallet_id integer
)
language plpgsql
as $$
declare
    counts record;
begin
    select
        count(tx_id) as total_count,
        count(tx_id) filter (
            where p_wallet_id = any(wallets)
        ) as total_wallet_count,
        count(tx_id) filter (
            where
                p_wallet_id = any(wallets) and
                array_length(related_accounts, 1) is null
        ) as wallet_count
    into
        counts
    from
        tx_ref
    where
        user_account_id = p_user_account_id;

    insert into stats (user_account_id, tx_count) values (
        p_user_account_id, counts.total_count
    ) on conflict (user_account_id) do update set
        tx_count = counts.total_count;

    update wallet set
        total_tx_count = counts.total_wallet_count,
        tx_count = counts.wallet_count
    where
        id = p_wallet_id;
end;
$$;

-- TODO: this should probably be split into 2 different methods based on if
-- related_account_address is null or not
create procedure set_user_transactions(
    p_user_account_id integer,
    p_tx_ids varchar[],
    p_wallet_id integer,
    p_related_account_address varchar(64) default null
)
language plpgsql
as $$
declare
    update_count integer;
begin
    if p_related_account_address is not null then
        insert into solana_related_account (
            address, wallet_id
        ) values (
            p_related_account_address, p_wallet_id
        ) on conflict (
            address, wallet_id
        ) do nothing;
    end if;

    insert into tx_ref (
        user_account_id,
        tx_id,
        wallets,
        related_accounts
    ) select
        p_user_account_id,
        t.tx_id,
        array[p_wallet_id],
        case when p_related_account_address is not null then
            array[p_related_account_address]
        else
            array[]::varchar(64)[]
        end
    from unnest(
        p_tx_ids
    ) as t(tx_id)
    on conflict (
        user_account_id, tx_id
    ) do update set
        wallets = array_append_unique(tx_ref.wallets, p_wallet_id),
        related_accounts = array_append_unique(tx_ref.related_accounts, p_related_account_address);

    get diagnostics update_count = row_count;

    if update_count > 0 then
        call set_user_txs_count(p_user_account_id, p_wallet_id);
    end if;
end;
$$;

create procedure dev_delete_user_transactions(
    p_user_account_id integer,
    p_wallet_id integer
)
language plpgsql
as $$
declare
begin
    delete from tx_ref t where
        array_length(t.wallets, 1) = 1 and
        t.wallets[1] = p_wallet_id;

    update tx_ref set
        wallets = array_remove(tx_ref.wallets, p_wallet_id)
    where
        p_wallet_id = any(tx_ref.wallets);

    delete from solana_related_account s where
        s.wallet_id = p_wallet_id;

    call set_user_txs_count(p_user_account_id, p_wallet_id);
end;
$$;

commit;
