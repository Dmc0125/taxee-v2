create table sync_request (
    id serial primary key,
    timestamp timestamptz not null default now(),
    user_account_id integer not null,
    type smallint not null,
    status smallint not null,

    foreign key (user_account_id) references user_account (id)
);
