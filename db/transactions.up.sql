begin;

create type network as enum ('solana');

create table stats (
    user_account_id integer not null,
    primary key (user_account_id),
    foreign key (user_account_id) references user_account (id) on delete cascade,

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

    tx_count integer default 0,
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
    fee bigint not null,
    signer varchar(64) not null,
    fee_payer varchar(64) not null,
    timestamp timestamptz not null,
    -- network = solana -> SolanaTxData
    data jsonb not null
);

create table tx_ref (
    user_account_id integer not null,
    wallet_id integer not null,
    tx_id varchar not null,
    related_account_address varchar(64),

    primary key (user_account_id, wallet_id, tx_id),

    foreign key (user_account_id) references user_account (id),
    foreign key (
        wallet_id, related_account_address
    ) references solana_related_account (
        wallet_id, address
    ) on delete cascade, 
    foreign key (wallet_id) references wallet (id) on delete cascade
);

create procedure set_user_transactions(
    p_user_account_id integer,
    p_tx_ids varchar[],
    p_wallet_id integer,
    p_related_account_address varchar(64) default null
)
language plpgsql
as $$
declare
    refs_count integer;
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
        wallet_id,
        related_account_address,
        tx_id
    ) select
        p_user_account_id,
        p_wallet_id,
        p_related_account_address,
        t.tx_id
    from unnest(p_tx_ids) as t(tx_id)
    on conflict (
        user_account_id, wallet_id, tx_id
    ) do nothing;

    get diagnostics refs_count = row_count;

    if refs_count > 0 then
        update stats set
            tx_count = tx_count + refs_count
        where user_account_id = p_user_account_id;

        if p_related_account_address is not null then
            update wallet set
                total_tx_count = total_tx_count + refs_count
            where id = p_wallet_id;
        else
            update wallet set
                tx_count = tx_count + refs_count,
                total_tx_count = total_tx_count + refs_count
            where id = p_wallet_id;
        end if;
    end if;
end;
$$;

commit;
