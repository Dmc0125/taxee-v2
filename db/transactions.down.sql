begin;

drop table tx_ref;

drop table tx;

drop table solana_related_account;

drop function set_wallet;

drop table wallet;

drop type network;

drop table stats; 

drop procedure dev_delete_user_transactions;

drop procedure set_user_transactions;

drop procedure set_user_txs_count;

drop function array_append_unique;

drop function tx_ref_delete_related_account;

drop function tx_ref_delete_wallet;

commit;
