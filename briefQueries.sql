-- get Source id
select id from listSources where title='Acta physica Polonica A';

-- Get count Papers from a source and year
select count(*) from listPapers where source=3226 and year=2010;