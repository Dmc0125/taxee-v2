begin;

drop table tx_ref;

drop table tx;

drop table solana_related_account;

drop function dev_set_wallet;

drop table wallet;

drop type network;

drop table stats; 

drop function tx_ref_delete_related_account;

drop function tx_ref_delete_wallet;

commit;
