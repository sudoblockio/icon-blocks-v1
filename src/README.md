

API: `api/main.go`

Worker: `api/main.go`

Swagger: 
```bash
go get github.com/swaggo/swag/cmd/swag; \
go get github.com/alecthomas/template; \
go get github.com/riferrei/srclient@v0.3.0; \
cd src && swag init -g api/routes/api.go
```
