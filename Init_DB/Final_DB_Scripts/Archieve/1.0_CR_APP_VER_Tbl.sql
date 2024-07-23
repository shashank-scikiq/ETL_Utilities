CREATE TABLE if not exists POSTGRES_SCHEMA.ONDC_DASHBOARD_VERSION_TBL (
	id serial4 NOT NULL PRIMARY,
	major int4 NOT NULL,
	minor int4 NOT NULL,
	minor_minor int4 NOT NULL,
	run_date timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	hot_fix_file_name varchar(100) DEFAULT NULL::character varying NULL,
	CONSTRAINT ondc_dashboard_version_pkey PRIMARY KEY (id));