begin;

drop procedure dev_delete_parsed;

drop table event;

drop type event_type;

drop table pricepoint;

drop table coingecko_token;

drop table coingecko_token_data;

drop table parser_err;

drop type parser_err_type;

drop type parser_err_origin;

commit;
