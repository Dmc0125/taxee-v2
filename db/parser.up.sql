begin;

create table iteration (
    id serial primary key,
    label varchar(40),
    timestamp timestamptz not null default now()
);

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
    iteration_id integer not null,

    tx_id varchar not null,
    ix_idx integer,
    idx integer not null, 

    origin err_origin not null,

    type err_type not null,
    address varchar(64) not null,
    data jsonb, 

    foreign key (iteration_id) references iteration (id) on delete cascade,
    foreign key (tx_id) references tx (id),
    unique (iteration_id, tx_id, ix_idx, idx)
);

create function get_user_txs(
    p_slot bigint,
    p_block_index integer,
    p_limit integer
) returns setof table(
    id varchar,
    err boolean,
    fee bigint,
    signer varchar(64),
    timestamp timestamptz,
    data jsonb,
    errs jsonb
)
language plpgsql
as $$
declare
begin
    return query
    select
        tx.id, tx.err, tx.fee, tx.signer, tx.timestamp, tx.data,
        array_agg(
            row(
                err.ix_idx,
                err.idx,
                err.origin,
                err.type,
                err.address,
                err.data
            ) order by err.idx
        ) filter (where err.id is not null) as errs
    into
        txs
    from
        tx
    left join
        err on err.tx_id = tx.id
    where
        (
            (tx.data->>'slot')::bigint = p_slot and
            (tx.data->>'blockIndex')::integer > p_block_index 
        ) or 
        (tx.data->>'slot')::bigint > p_slot
    order by
        (tx.data->>'blockIndex')::integer asc,
        (tx.data->>'slot')::bigint asc
    limit
        p_limit;
end;
$$;

commit;
