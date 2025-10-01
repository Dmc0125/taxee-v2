.PHONY: tw

tw-watch:
	bunx @tailwindcss/cli -i website/input.css -o website/static/output.css --watch

ws-watch:
	watchexec -r --watch ./website --exts go,html,css -- 'go run website/main.go' 
