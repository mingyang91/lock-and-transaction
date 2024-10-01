-- Add up migration script here

create table accounts(
    address varchar(35) primary key,
    balance int8 not null, -- check ( balance >= 0 ),
    updated_at timestamp not null default current_timestamp
);

create table transaction(
    tx_hash varchar(64) primary key,
    from_address varchar(35) not null references accounts(address),
    to_address varchar(35) not null references accounts(address),
    amount int8 not null check ( amount >= 0 ),
    created_at timestamp not null default current_timestamp
);

create index idx_transaction_from_address on transaction(from_address);
create index idx_transaction_to_address on transaction(to_address);