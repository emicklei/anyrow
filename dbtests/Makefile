rmdb:
	docker rm -f anyrowdb

db:
	# make sure to create an empty database called "anyrowdb"
	#
	docker run --name anyrowdb -e POSTGRES_PASSWORD=anyrowdb  -p 7432:5432 -d postgres

test:
	PGTALK_CONN=postgres://postgres:anyrowdb@localhost:7432/postgres go test -v

