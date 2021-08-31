DROP schema IF EXISTS otus;
create schema otus;

create table distance_statistics
(
    date text,
    count bigint not null,
    min double precision,
    max double precision,
    avg double precision,
    "RMSD(trip_distance)" double precision
);

