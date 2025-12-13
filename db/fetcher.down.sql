begin;

drop procedure set_transactions_for_related_account;

drop procedure set_transactions_for_wallet;

drop procedure update_solana_wallet_tx_count;

drop table tx_ref;

drop table tx;

drop table solana_related_account;

drop table wallet;

drop type network;

drop type status;

commit;
