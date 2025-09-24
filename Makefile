init_apache:
	docker compose exec superset superset db upgrade

	docker compose exec superset superset fab create-admin \
	--username admin \
	--firstname Superset \
	--lastname Admin \
	--email admin@example.com \
	--password superset

	docker compose exec superset superset init
