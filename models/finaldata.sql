{{ config(materialized='table') }}


with transform_liveability as (
SELECT * 
FROM
	(

	SELECT
		 ccare.name as name   
		,ccare.Categories as category    
		,ccare.Address as address    
		,ccare.City as city     
		,ccare.Postcode as postcode    
		,ccare.Latitude as Latitude
		,ccare.Longitude as longitude
	FROM liveability.childcarecentre ccare
	UNION ALL
	SELECT
		 hosp.name as name   
		,hosp.Categories as category    
		,hosp.Address as address    
		,hosp.City as city     
		,hosp.Postcode as postcode    
		,hosp.Latitude as Latitude
		,hosp.Longitude as longitude
	FROM liveability.hospitals hosp
	UNION ALL 
	SELECT
		 rel.name as name   
		,rel.Category as category    
		,rel.Address as address    
		,rel.Suburb as city     
		,rel.Postcode as postcode    
		,rel.Latitude as Latitude
		,rel.Longitude as longitude
	FROM liveability.religious_place rel 
	UNION ALL 
	SELECT
		 res.name as name   
		,res.Categories as category    
		,res.Address as address    
		,res.City as city     
		,res.Postcode as postcode    
		,res.Latitude as Latitude
		,res.Longitude as longitude
	FROM liveability.restaurants res 
	UNION ALL 
	SELECT
		 sch.name as name   
		,sch.Categories as category    
		,sch.Address as address    
		,sch.City as city     
		,sch.Postcode as postcode    
		,sch.Latitude as Latitude
		,sch.Longitude as longitude
	FROM liveability.schools sch
	UNION ALL  
	SELECT
		 shop.name as name
		,shop.Categories as category
		,shop.Address as adress
		,shop.City as city
		,shop.Postcode as postcode
		,shop.Latitude as latitude
		,shop.Longitude as longitude
	FROM liveability.shoppingcentre shop
	UNION ALL 
	SELECT
		 sport.name as name
		,sport.Categories as category
		,sport.Address as adress
		,sport.City as city
		,sport.Postcode as postcode
		,sport.Latitude as latitude
		,sport.Longitude as longitude
	FROM liveability.sportingclubs sport

)
WHERE postcode = 
              (WITH T AS 
								(
								SELECT ui.Name as Name
      					        ,ui.New_Postcode as Postcode
      				        	,RANK() over (
        			        	ORDER BY ui.Timestamp DESC
      				        	            ) AS RANK_NO
								FROM `streamdata.user_input` ui
								)
							
							SELECT T.Postcode
							FROM T
			    			WHERE T.RANK_NO = 1
				)

)
select * from transform_liveability

