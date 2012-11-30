-- get Source id
select id from listSources where title='Acta physica Polonica A';

-- Get count Papers from a source and year
select count(*) from listPapers where source=3226 and year=2010;

-- Get Sources requested -- Inserted into a table
create table tmp_listSources select * from listSources where title in ('Acta physica Polonica A','Central European journal of physics','Foundations of physics','Physics letters A','Physica A-Statistical mechanics and its applications','Science in China Series G-Physics Mechanics & astronomy /','Acta physica Polonica B','Acta physica Sinica','Acta physica Slovaca','American journal of physics','Annales Henri Poincare','Annalen der Physik','Annals of physics','Annales de physique','Bulletin of the Lebedev Physics Institute','Brazilian journal of physics','Canadian journal of physics','Chaos solitons & fractals','Chinese journal of physics','Chinese physics B','Chinese physics letters','Classical and quantum gravity','Communications in theoretical physics','Contemporary physics','Comptes rendus Physique','Doklady Physics','Entropy','Europhysics letters','European journal of physics','European physical journal H','European physical journal plus','European physical journal-Special topics','Few-body systems','Fortschritte der Physik','Frontiers of physics in China','Frontiers of physics','General relativity and gravitation','High pressure research','Indian journal of physics','Indian Journal of Pure & Applied Physics','International journal of theoretical physics','Journal of Contemporary Physics-Armenian Academy of Sciences','Journal of experimental and theoretical physics','Journal of the Korean Physical Society','Journal of physics A-Mathematical and theoretical','Journal of physical and chemical reference data','Journal of the Physical Society of Japan','JETP letters','Lithuanian journal of physics','Moscow University physics bulletin','Nature physics','New journal of physics','NUOVO CIMENTO DELLA SOCIETA ITALIANA DI FISICA B-Basic Topics in Physics','Physics essays','Physics letters B','Physica scripta','Physics today','Physics of wave phenomena','Physics world','Physics-Uspekhi','Physica D-Nonlinear phenomena','PRAMANA-JOURNAL OF PHYSICS','Progress of theoretical physics','Progress of theoretical physics Supplement','Quantum information processing','Reports on progress in physics','Revista Brasileira de Ensino de Fisica','Revista mexicana de fisica','Revista Mexicana de Fisica E','Reviews of modern physics','rivista del Nuovo cimento','Romanian journal of physics','Romanian reports in physics','Russian physics journal','Science China-Physics mechanics & astronomy','Soft matter','Studies in history and philosophy of modern physics','Theoretical and mathematical physics','University Politehnica of Bucharest Scientific bulletin-Series A-Applied mathematics and physics','Wave motion','Waves in random and complex media','Zeitschrift fur Naturforschung Section A-A journal of physical sciences','Science China-Physics Mechanics & Astronomy');

-- Get Count papers from list sources;
select count(*),s.id, s.title from listSources s inner join listPapers p on s.id=p.source group by s.id, s.title;

-- Get count cites in
select count(*), s.title from tmp_listSources s inner join (listPapers p inner join paperPaper_aso pa on p.id=pa.dstId ) on p.source=s.id group by s.id, s.title;


 -- Get papers completed
 select count(*) as count,s.id,  s.title from tmp_listSources s inner join (listPapers p inner join papersCitesDownload pc on p.id=pc.paperId and pc.download=true and pc.cite='in' ) on p.source=s.id group by s.id, s.title;

 -- Get % downloaded
 select t.count, d.count, t.id, t.title from tmp_downloadTotal t inner join tmp_downloaded d on t.id=d.id order by t.title;