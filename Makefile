.PHONY: 

tw-watch:
	bunx @tailwindcss/cli -i website/input.css -o website/static/output.css --watch

ws-watch:
	watchexec -r --watch ./website --exts go,html,css -- 'HTML=/home/dmnk/apps/taxee/website/html go run ./website/src/.' 

migrate.%:
	go run cmd/main.go migrate ./db/$*.sql

parse:
	go run cmd/main.go parse

worker:
	go run cmd/worker/main.go
