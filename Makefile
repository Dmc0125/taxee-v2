.PHONY: tw

tw-watch:
	bunx @tailwindcss/cli -i website/input.css -o website/static/output.css --watch

ws-watch:
	watchexec -r --watch ./website --exts go,html,css -- 'HTML=/home/dmnk/apps/taxee/website/html go run ./website/.' 

migrate.%:
	go run cmd/main.go migrate ./db/$*.sql

parse:
	go run cmd/main.go parse

parser-server:
	go run cmd/parser_server/main.go
