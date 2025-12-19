begin;

create type wallet_status as enum (
    'queued',
    'in_progress',
    'success',
    'error',
    'delete'
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
    label varchar,

    unique (id, user_account_id),

    tx_count integer default 0,
    data jsonb not null,

    status wallet_status not null default 'queued',
    fresh boolean not null default false,

    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    queued_at timestamptz,
    started_at timestamptz,
    finished_at timestamptz
);

create table solana_related_account (
    id serial primary key,

    user_account_id integer not null,
    foreign key (user_account_id) references user_account (id) on delete cascade,
    wallet_id integer not null,
    foreign key (wallet_id) references wallet (id) on delete cascade,

    address varchar(64) not null,
    unique (wallet_id, address),

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
    id serial primary key,

    user_account_id integer not null,
    tx_id varchar not null,
    foreign key (user_account_id) references user_account (id),
    foreign key (tx_id) references tx (id) on delete cascade,

    wallet_id integer,
    related_account_id integer,

    foreign key (wallet_id) references wallet (id) on delete cascade,
    foreign key (related_account_id) references solana_related_account (id) on delete cascade,

    unique (user_account_id, tx_id, wallet_id, related_account_id)
);

create procedure update_solana_wallet_tx_count(
    p_user_account_id integer,
    p_wallet_id integer
)
language plpgsql
as $$
declare
    v_count integer;
begin
    select count(distinct tr.tx_id) into v_count
    from tx_ref tr where
        tr.user_account_id = p_user_account_id and
        (
            (tr.wallet_id is not null and tr.wallet_id = p_wallet_id) or
            (tr.wallet_id is null and
                exists (
                    select 1 from solana_related_account r where
                        r.id = tr.related_account_id and
                        r.wallet_id = p_wallet_id and
                        r.user_account_id = p_user_account_id
                )
            )
        );

    update wallet set tx_count = v_count where
        id = p_wallet_id and user_account_id = p_user_account_id;
end;
$$;

create procedure set_transactions_for_wallet(
    p_user_account_id integer,
    p_tx_ids varchar[],
    p_wallet_id integer
)
language plpgsql
as $$
declare
    v_update_count integer;
begin
    insert into tx_ref (
        user_account_id, tx_id, wallet_id
    ) select
        p_user_account_id, t.tx_id, p_wallet_id
    from 
        unnest(p_tx_ids) as t(tx_id)
    on conflict (
        user_account_id, tx_id, wallet_id, related_account_id
    ) do nothing;

    get diagnostics v_update_count = row_count;

    if v_update_count > 0 then
        call update_solana_wallet_tx_count(p_user_account_id, p_wallet_id);
    end if;
end;
$$;

create procedure set_transactions_for_related_account(
    p_user_account_id integer,
    p_tx_ids varchar[],
    p_wallet_id integer,
    p_related_account_address varchar
)
language plpgsql
as $$
declare
    v_account_id integer;
    v_update_count integer;
begin
    select id into v_account_id from solana_related_account where
        address = p_related_account_address and
        user_account_id = p_user_account_id;

    if v_account_id is null then
        insert into solana_related_account (
            user_account_id, wallet_id, address
        ) values (
            p_user_account_id, p_wallet_id, p_related_account_address
        ) returning id into v_account_id;
    end if;

    insert into tx_ref (
        user_account_id, tx_id, related_account_id
    ) select
        p_user_account_id, t.tx_id, v_account_id
    from 
        unnest(p_tx_ids) as t(tx_id)
    on conflict (
        user_account_id, tx_id, wallet_id, related_account_id
    ) do nothing;

    get diagnostics v_update_count = row_count;

    if v_update_count > 0 then
        call update_solana_wallet_tx_count(p_user_account_id, p_wallet_id);
    end if;
end;
$$;

commit;
