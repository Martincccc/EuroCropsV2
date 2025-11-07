-- generate the full europe gSA layer for YeaR_
drop table if exists out.gsa_YeaR_centroid ;

-- Centroid
CREATE TABLE out.gsa_YeaR_centroid AS 
SELECT nuts,cropfield,hcat4_code,usage_code,original_code,area_ha,st_centroid(geom) geom
from out.gsa_YeaR ;

CREATE INDEX ON out.gsa_YeaR_centroid USING GIST (geom);
CREATE INDEX ON out.gsa_YeaR_centroid (nuts);
CREATE INDEX ON out.gsa_YeaR_centroid (cropfield);
CREATE INDEX ON out.gsa_YeaR_centroid (hcat4_code);
CREATE INDEX ON out.gsa_YeaR_centroid (original_code);
CREATE INDEX ON out.gsa_YeaR_centroid (usage_code);
CREATE INDEX ON out.gsa_YeaR_centroid (area_ha);

DROP TABLE IF EXISTS out.admin_YeaR ;
CREATE TABLE out.admin_YeaR AS
SELECT * FROM public.nuts2021 n WHERE n.nuts_id IN
(SELECT upper(split_part(table_name,'_',1))
FROM information_schema.columns 
WHERE table_name LIKE '%_YeaR' 
AND table_schema = 'gsa'
AND column_name = 'cropfield') ;

CREATE INDEX ON out.admin_YeaR USING GIST (wkb_geometry);

create temp table grid_intersect_YeaR as
select g.grd_id ,gs.hcat4_code, 
sum(st_area(st_intersection(gs.geom , g.geom) ))/10000 area_ha ,
count(gs.cropfield) counts
from out.gsa_YeaR gs,grid.grid_10k g
where st_intersects(gs.geom , g.geom) 
group by g.grd_id ,gs.hcat4_code ;

CREATE INDEX ON grid_intersect_YeaR (grd_id);
CREATE INDEX ON grid_intersect_YeaR (hcat4_code);
CREATE INDEX ON grid_intersect_YeaR (area_ha);

drop table if exists grid.grid_gsa_YeaR cascade;
create table grid.grid_gsa_YeaR as
select a.*,g.geom 
from grid.grid_10k g, grid_intersect_YeaR a
where a.grd_id=g.grd_id; 

CREATE INDEX ON grid.grid_gsa_YeaR USING GIST (geom); 
CREATE INDEX ON grid.grid_gsa_YeaR (hcat4_code);  
CREATE INDEX ON grid.grid_gsa_YeaR (grd_id);  
CREATE INDEX ON grid.grid_gsa_YeaR (area_ha);


