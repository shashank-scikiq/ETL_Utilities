INSERT into POSTGRES_SCHEMA.DIM_DISTRICTS_TBL(delivery_state, delivery_district)
SELECT distinct "Statename" as delivery_state, "Districtname" as delivery_district
	FROM POSTGRES_SCHEMA.TBL_PINCODE
	GROUP BY "Statename",  "Districtname"