-- start_ignore
create extension pax;
drop table if exists users;
-- end_ignore
create table users(
    id int ,
    name text not null,
    height float not null,
    decimal_col decimal(10, 2) not null,
    created_at timestamp with time zone not null,
    updated_at timestamp with time zone not null
)  using pax distributed BY (id);

insert into users (id, name, height, decimal_col, created_at, updated_at) values
    (1, 'Alice', 1.65, 1.23, '2023-05-17 17:56:49.633664+08', '2023-05-17 17:56:49.633664+08'),
    (2, 'Bob', 1.75, 2.34, '2023-05-17 17:56:49.633664+08', '2023-05-17 17:56:49.633664+08'),
    (3, 'Carol', 1.85, 3.45, '2023-05-17 17:56:49.633664+08', '2023-05-17 17:56:49.633664+08');
select * from users;

DELETE FROM users WHERE id = 1;
select * from users;

UPDATE users SET name = 'Alice' WHERE id = 2;
select * from users;

UPDATE users SET height = (select max(height) from users),decimal_col = (select min(decimal_col) from users);
select * from users;
