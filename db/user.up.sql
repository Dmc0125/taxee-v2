begin;

create table user_account (
    id serial primary key,
    name varchar not null
);

-- TODO: implement later
-- create type oauth_provider as enum (
--     'google'
-- );
--
-- create table user_oauth (
--     provider oauth_provider not null,
--     user_id integer not null,
--
--     primary key (provider, user_id),
--     foreign key (user_id) references user (id)
--
--     provider_id varchar not null,
-- );

commit;
