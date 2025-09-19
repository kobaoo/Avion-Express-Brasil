init_apache:
	docker compose exec superset superset db upgrade
	docker compose exec superset superset init	