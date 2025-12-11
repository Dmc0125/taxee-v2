begin;

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
        count(tr.tx_id) as total_count,
        count(tr.tx_id) filter (
            where p_wallet_id = any(tr.wallets)
        ) as wallet_count
    into
        counts
    from
        tx_ref tr
    where
        tr.user_account_id = p_user_account_id;

    update stats set
        tx_count = counts.total_count
    where
        user_account_id = p_user_account_id;

    update wallet set
        tx_count = counts.wallet_count
    where
        id = p_wallet_id and user_account_id = p_user_account_id;
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

create procedure delete_user_transactions(
    p_user_account_id integer,
    p_wallet_id integer
)
language plpgsql
as $$
declare
    v_wallet_found boolean;
begin
    -- validation
    select true into v_wallet_found from wallet where
        user_account_id = p_user_account_id and
        id = p_wallet_id;

    if not v_wallet_found then
        raise exception 'wallet not found'; 
    end if;

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

create procedure delete_wallet(
    p_user_account_id integer,
    p_wallet_id integer
)
language plpgsql
as $$
declare
begin
    call delete_user_transactions(p_user_account_id, p_wallet_id);

    delete from wallet where 
        id = p_wallet_id and 
        user_account_id = p_user_account_id;
end;
$$;

commit;
