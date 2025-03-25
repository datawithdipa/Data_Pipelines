drop table raw.public.earthquake;

create table raw.public.earthquake
(
    ts varchar(100),
    place varchar(100),
    magnitude numeric(32,8),
    longitude numeric(32,8),
    latitude numeric(32,8),
    depth numeric(32,8),
    filename varchar(100)
);

delete from earthquake;

select * from earthquake;
