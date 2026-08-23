.PHONY: up down seed psql reset demo-broken demo-fixed test

up:                ## start the source database and wait for it
	docker compose up -d
	@until docker compose exec -T source-db pg_isready -U pipeline -d marketplace >/dev/null 2>&1; \
		do echo "waiting for source-db..."; sleep 1; done
	@echo "source-db ready on localhost:5433"

seed:              ## generate the deterministic 7-day window
	python -m src.generator.seed --start 2026-08-01 --days 7 --orders 5000 --seed 42

psql:
	docker compose exec source-db psql -U pipeline -d marketplace

reset: down up seed  ## nuke and regenerate

down:
	docker compose down -v

# --- filled in at build steps 2-5 -------------------------------------------------
demo-broken:       ## naive: stateful watermark + append. Run twice, watch balances double.
	@echo "not implemented yet — build step 5"

demo-fixed:        ## interval-bound + guarded MERGE. Run twice, nothing changes.
	@echo "not implemented yet — build step 3"

test:
	pytest -q
