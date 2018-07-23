convert:
	docker-compose \
		run gdal \
		python3 /data/netcdf-cog.py \
			--path /data/data \
			--output /data/output

build:
	docker-compose build