# Prometheus-sketch

## Testing the code
```
go build
go test
```

## Benchamrks
### Setup

Prepare the dependency:
```
$ go mod vendor
```

Compile:
```
go build cmd/main.go
```
or 
```
make build
```

test single file:
```
go test -v xxx_test.go xxx.go
```